<tr class="table_row" height=27 >
    <td align=right class="table_row">
	<<:com_job_start:>><a href="/raw/raw_details.jsp?filter_runno=<<:run db enc:>>&filter_lpm=0"><u><<:com_job_end:>><span onMouseOver="runDetails(<<:run db esc js:>>);" onMouseOut="nd()" onClick="nd(); return true"><<:run db esc:>></span><<:com_job_start:>></u></a><<:com_job_end:>>
    </td>
    <td align=right class="table_row" sorttable_customkey="<<:partition db:>>"><<:partition db esc:>></td>
    <td align=right class="table_row"><<:global_run_type esc:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:chunks db:>>"><a href="details.jsp?runfilter=<<:run db enc:>>&time=0"><<:chunks db esc:>></a></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:avg_file_size db:>>"><<:avg_file_size db size:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:size db:>>"><<:size db size:>></td>
    <td align=right class="table_row" sorttable_customkey="<<:ctf_file_count esc:>>"><<:xctf_file_count esc:>></td>
    <td align=right class="table_row" sorttable_customkey="<<:ctf_file_size:>>"><<:xctf_file_size size:>></td>
    <td align=right class="table_row"><<:ctf_replica esc:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:tf_file_count esc:>>"><<:xtf_file_count esc:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:tf_file_size:>>"><<:xtf_file_size size:>></td>
    <td align=right class="table_row"><<:tf_replica esc:>></td>
    <td align=right class="table_row" sorttable_customkey="<<:other_file_count esc:>>"><<:xother_file_count esc:>></td>
    <td align=right class="table_row" sorttable_customkey="<<:other_file_size:>>"><<:xother_file_size size:>></td>
    <td align=right class="table_row"><<:other_replica esc:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:mintime db:>>"><<:mintime db nicedate:>> <<:mintime db time:>></td>
    <td align=right class="table_row" bgcolor="#F6F6F6" sorttable_customkey="<<:maxtime db:>>"><<:maxtime db nicedate:>> <<:maxtime db time:>></td>
    <td align=right class="table_row" bgcolor="<<:transferstatus_bgcolor esc:>>"><a class=link href="/transfers/?id=<<:transfer_id db enc:>>"><<:targetse db esc:>></a></td>
    <td align=right class="table_row" bgcolor="<<:processingstatus_bgcolor esc:>>">
	<<:com_processing_flag_start:>>
	<<:com_errorv_start:>>
	    <a href="details.jsp?runfilter=<<:run db enc:>>&time=0"><img src="/img/trend_alert.png" border=0 onMouseOver="overlib('<<:errorv_count db esc js:>> subjobs have failed with ERROR_V');" onMouseOut="nd();"></a>&nbsp;
	<<:com_errorv_end:>>
	<span onMouseOver="overlib('<<:esds_path esc js:>>');" onClick="showCenteredWindow('<<:esds_path esc js:>>', 'ESDs path');" onMouseOut='nd();'>
	    <<:processed_chunks db esc:>> / <<:chunks db esc:>> (<<:processing_percent db esc:>>%)
	</span>
	<<:com_processing_flag_end:>>
    </td>
    <!--
    <td align=center class="table_row" bgcolor="<<:stagingstatus_bgcolor esc:>>" id="td_<<:counter esc:>>">&nbsp;
	<<:com_order_start:>><input id="button_<<:counter esc:>>" type=button class="input_submit" onClick="order(<<:run db esc js:>>, <<:counter esc js:>>);" value="Stage"><<:com_order_end:>>
	<<:com_remove_start:>><input id="remove_<<:counter esc:>>" type=button class="input_submit" onClick="deleteRun(<<:run db esc js:>>, <<:counter esc js:>>);" value="Dismiss"><<:com_remove_end:>>
    </td>
    -->
    <td align=right class="table_row" sorttable_customkey="<<:runlength:>>">
	<<:runlength interval:>>
    </td>
    <td align=right class="table_row">
	<<:goodrun esc:>>
    </td>
    <td align=right class="table_row">
    <<:run_action esc:>>
    </td>
    <td align=right class="table_row">
	<<:com_processing_flag_auth_start:>>
	    <input type=hidden name=pfo value=<<:run db esc:>>>
	<<:com_processing_flag_auth_end:>>
    </td>
</tr>
