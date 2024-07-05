CREATE OR REPLACE PACKAGE PKG_EBOOK_RTL_MGMT IS

  --Author: DynaK
  --Purpose: Perform several day end record management of ebook retail store
  --Created: July 2024
  
  PROCEDURE P_PROCESS_MAIN(O_RESULT OUT NUMBER,
						   O_INFO   OUT VARCHAR2);
	
  PROCEDURE P_REWARD_LOYALTY(O_RESULT OUT NUMBER,
						     O_INFO   OUT VARCHAR2);

  PROCEDURE P_PENDING_ORDER_CHKUPD(I_TRANSACTION_DATE  IN DATE,
								   O_RESULT OUT NUMBER);

  PROCEDURE P_DISCNT_RATE_ADJUST(O_RESULT OUT NUMBER,
						         O_INFO   OUT VARCHAR2);

  PROCEDURE P_CUST_CLEANUP(I_CNT_ID  IN NUMBER);

  PROCEDURE P_RUN_JOB(I_PROCESS_DATE IN DATE);  
						  
END PKG_EBOOK_RTL_MGMT;
/

CREATE OR REPLACE PACKAGE BODY PKG_EBOOK_RTL_MGMT IS
  
  TYPE TYPE_ORDER     IS TABLE OF ORDERS%ROWTYPE;

  PROCEDURE P_PROCESS_MAIN(O_RESULT OUT NUMBER, O_INFO   OUT VARCHAR2) AS
	BEGIN
	
	DBMS_OUTPUT.PUT_LINE(':: Begin ::');
	
	P_REWARD_LOYALTY(O_RESULT, O_INFO);
	P_PENDING_ORDER_CHKUPD(sysdate, O_RESULT);
	P_DISCNT_RATE_ADJUST(O_RESULT, O_INFO);
	P_RUN_JOB(sysdate);
	
	DBMS_OUTPUT.PUT_LINE(':: End ::');
  
  END P_PROCESS_MAIN;
  
  	
  PROCEDURE P_REWARD_LOYALTY(O_RESULT OUT NUMBER, O_INFO   OUT VARCHAR2) AS
    
	CURSOR c_active IS
	  SELECT DISTINCT C.CUSTOMER_ID, C.LOYALTY_POINT 
	  FROM CUSTOMERS C, ORDERS O
	  WHERE TRUNC(SYSDATE) - O.ORDER_DATE > 4 AND O.CUST_NO = C.CUSTOMER_ID;
	  
	  V_CUST_ID    	        CUSTOMERS.CUSTOMER_ID%TYPE;
	  V_LOYAL_POINT         CUSTOMERS.LOYALTY_POINT%TYPE;
	  V_CNT                 NUMBER(5);
	  V_NEW_LOYALTY_POINT   NUMBER(5);
	
	BEGIN
      
	  OPEN c_active;
	  LOOP
	  
		FETCH c_active INTO V_CUST_ID, V_LOYAL_POINT;
		EXIT WHEN c_active%notfound;
		
		BEGIN
		SAVEPOINT POINT_1;
		
		IF V_LOYAL_POINT IS NOT NULL THEN
			SELECT COUNT(*) INTO V_CNT FROM ORDERS WHERE CUST_NO = V_CUST_ID AND ORDER_NO IN 
			  (SELECT ORDER_NO FROM ORDER_DETAILS GROUP BY ORDER_NO HAVING SUM(QTY*PRICE)>100);
			  
			V_NEW_LOYALTY_POINT   := 0;
			
			IF V_CNT > 1 THEN 
				V_NEW_LOYALTY_POINT := V_LOYAL_POINT + 3;
			ELSE
				V_NEW_LOYALTY_POINT := V_LOYAL_POINT + 1;
			END IF; 
			
			UPDATE CUSTOMERS SET LOYALTY_POINT = V_NEW_LOYALTY_POINT WHERE CUSTOMER_ID = V_CUST_ID;
		
		END IF;
		
		EXCEPTION
			WHEN OTHERS THEN 
			    ROLLBACK TO POINT_1;
				O_RESULT := 1;
				O_INFO   := SQLERRM;
		END;		
	  
	  END LOOP;
	  CLOSE c_active;
	  
	  COMMIT;
	  
	  IF O_RESULT IS NULL THEN
		O_RESULT := 0;
		O_INFO := 'SUCCESS';
	  END IF;
  
  END P_REWARD_LOYALTY;
   

  PROCEDURE P_PENDING_ORDER_CHKUPD(I_TRANSACTION_DATE  IN DATE, O_RESULT OUT NUMBER) AS
  
    CURSOR c_pending_order(I_TRANS_DATE  IN DATE) IS
	  SELECT * FROM ORDERS WHERE SHIP_DATE IS NULL AND TRUNC(ORDER_DATE) = I_TRANS_DATE;
    
	  V_PO_REC             TYPE_ORDER;
	  V_TRANSACTION_DATE   DATE;
	
	BEGIN
		V_TRANSACTION_DATE := TRUNC(I_TRANSACTION_DATE);
		O_RESULT := 0;
		
		OPEN c_pending_order(V_TRANSACTION_DATE);
		    
			FETCH c_pending_order BULK COLLECT INTO V_PO_REC;
			
				FORALL i IN V_PO_REC.FIRST..V_PO_REC.LAST
				  UPDATE ORDERS SET SHIP_DATE = TRUNC(SYDATE+1) WHERE ORDER_NO = V_PO_REC(i).ORDER_NO;
		
		CLOSE c_pending_order;
		
		COMMIT;
		
    EXCEPTION
	  WHEN OTHERS THEN 
	    O_RESULT := 1;
		DBMS_OUTPUT.PUT_LINE('- Error updating order record :: ' || SQLCODE || ': ' || SQLERRM);
  
  END P_PENDING_ORDER_CHKUPD;
  
  
  PROCEDURE P_DISCNT_RATE_ADJUST(O_RESULT OUT NUMBER, O_INFO   OUT VARCHAR2) AS
    --ilustrate code change after compile error
    CURSOR highcur IS
		SELECT ITEM_NO FROM PRODUCTS WHERE ITEM_NO IN
		(SELECT ITEM_NO FROM ORDER_DETAILS WHERE ORDER_NO IN(SELECT ORDER_NO FROM ORDERS WHERE TRUNC(SYSDATE) - ORDER_DATE <=7));
		/*OR ITEM_NO IN
		(SELECT ITEM_NO FROM ORDER_DETAILS GROUP BY ITEM_NO HAVING SUM(QTY) > 10)
		OR ITEM_NO IN
		(SELECT ITEM_NO, COUNT(*) FROM ORDER_DETAILS GROUP BY ITEM_NO HAVING COUNT(*) > 1);*/
	
	
	CURSOR lowcur IS
	    SELECT ITEM_NO FROM PRODUCTS WJERE ITEM_NO NOT IN
		(SELECT ITEM_NO FROM ORDER_DETAILS WHERE ORDER_NO IN 
		 (SELECT ORDER_NO FROM ORDERS WHERE TO_CHAR(ORDER_DATE,'MM/YY')=TO_CHAR(ADD_MONTHS(SYSDATE,-1),'MM/YY')));
   
	BEGIN
	
		O_RESULT := 0;
		O_INFO   := 'SUCCESS';
		
		--Already a popular item so reduce discount
		FOR REC IN highcur
		LOOP
		
			SAVEPOINT FIRST_SAVE;
			
			BEGIN
			
				UPDATE PRODUCTS SET DISCOUNT_RATE = DISCOUNT_RATE * 1.1 WHERE ITEM_NO = REC.ITEM_NO;
				
			EXCEPTION
				WHEN OTHERS THEN 
				    ROLLBACK TO FIRST_SAVE;
					O_RESULT := 1;
					O_INFO   := SQLERRM;
					DBMS_OUTPUT.PUT_LINE('- Error adjusting discount rate :: ' || SQLCODE || ': ' || SQLERRM);	
			END;		
		
		END LOOP;
		
		--No sale so give more discount
		FOR REC2 IN lowcur
		LOOP
		
			SAVEPOINT SECOND_SAVE;
			
			BEGIN
			
				UPDATE PRODUCTS SET DISCOUNT_RATE = DISCOUNT_RATE * 1.5 WHERE ITEM_NO = REC2.ITEM_NO;
				
			EXCEPTION
				WHEN OTHERS THEN 
				    ROLLBACK TO SECOND_SAVE;
					O_RESULT := 1;
					O_INFO   := SQLERRM;
					DBMS_OUTPUT.PUT_LINE('- Error increasing discount rate :: ' || SQLCODE || ': ' || SQLERRM);	
			END;		
		
		END LOOP;
		
		COMMIT;	
	
  END P_DISCNT_RATE_ADJUST;
    
  --This job will be called by scheduler with purpose removing record of customers without any order
  PROCEDURE P_CUST_CLEANUP(I_CNT_ID  IN NUMBER) AS
  
	BEGIN
	
		DELETE FROM CUSTOMERS WHERE CUSTOMER_ID NOT IN (SELECT CUST_NO FROM ORDERS);
		COMMIT;
		DBMS_OUTPUT.PUT_LINE('Inactive customer deletion job ' || I_CNT_ID || ' ran successfully');	
	
	EXCEPTION
				WHEN OTHERS THEN 
				    ROLLBACK;
					DBMS_OUTPUT.PUT_LINE('- Error deleting customers record :: ' || SQLCODE || ': ' || SQLERRM);	
	
  END P_CUST_CLEANUP;
  
  
  PROCEDURE P_RUN_JOB(I_PROCESS_DATE IN DATE) AS
  
    V_JOB_CNT     NUMBER:= 0;
	V_ALERT_ID    VARCHAR2(20);
	
	BEGIN
		
		V_JOB_CNT  := 1;
		V_ALERT_ID := 'DEL_CUST_' || V_JOB_CNT;

                --Add note: compile error will be thrown if current user has no sufficient db priviledge for dbms_alert
		DBMS_ALERT.REGISTER(V_ALERT_ID);
		
		DBMS_SCHEDULER.CREATE_JOB(job_name => V_ALERT_ID,
							      job_type => 'PLSQL_BLOCK',
								  job_action => 'BEGIN
								     PKG_EBOOK_RTL_MGMT.P_CUST_CLEANUP(' || V_JOB_CNT || ');
									 DBMS_ALERT.SIGNAL(''' || V_ALERT_ID || ''', ''COMPLETED!'');
									 COMMIT;
									 END;',
								  enabled => TRUE,
								  repeat_interval => 'FREQ=DAILY;BYHOUR=00;BYMINUTE=00',
								  auto_drop => TRUE,
								  comments => 'Job to run daily at 12 am'								 
								 );
								 
		COMMIT;
		
		DBMS_OUTPUT.PUT_LINE(V_ALERT_ID || ' job completed at ' || systimestamp ||' for run date = ' || I_PROCESS_DATE);	
	
	EXCEPTION
		WHEN OTHERS THEN 
			ROLLBACK;
			DBMS_OUTPUT.PUT_LINE(V_ALERT_ID || '- Error running the job :: ' || SQLCODE || ': ' || SQLERRM);		
	
  END P_RUN_JOB;
  
END PKG_EBOOK_RTL_MGMT;
/

