declare

o_result  number; 
o_info    varchar2;

begin
   pkg_ebook_rtl_mgmt.p_process_main(o_result => o_result,
								     o_info => o_info);
end;