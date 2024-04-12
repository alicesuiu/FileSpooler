<tr class="table_row" height=27>
<td align=right class="table_row" sorttable_customkey="<<:run db:>>"><a href="/DAQ/?time=0&runfilter=<<:run db enc:>>"><<:run db esc:>></a></td>
<td align=right class="table_row" sorttable_customkey="<<:runpartition esc:>>"><<:runpartition esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:action db:>>"><<:action db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:sourcese db:>>"><<:sourcese db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:filter db:>>"><<:filter db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:total_chunks esc:>>"><<:total_chunks esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:counter db:>>"><<:counter db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:total_size size:>>"><<:total_size size:>></td>
<td align=right class="table_row" sorttable_customkey="<<:size db:>>"><<:size db size:>></td>
<td align=right class="table_row" sorttable_customkey="<<:percentage db:>>"><<:percentage db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:source db:>>"><<:source db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:status db:>>"><<:status db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:addtime db:>>"><<:addtime db nicedate:>></td>
<td align=right class="table_row" sorttable_customkey="<<:log_message db:>>" onMouseOver="overlib('<<:log_message db js:>>')" onMouseOut="nd();" onClick="showCenteredWindow('<<:log_message db js:>>');"><<:log_message db esc cut10:>></td>
<td align=right class="table_row" style="text-align:center">
	<<:com_authenticated_start:>>
		<input type="checkbox" name="bulk_del" value="<<:id_record db esc:>>" onMouseOver="overlib('Mark run <<:run esc js:>> for deletion')" onMouseOut="nd();">
	<<:com_authenticated_end:>>
</td>
<td align=right class="table_row" style="text-align:center">
	<a href="https://alimonitor.cern.ch/deletion/del_request.jsp?id=<<:id_record db esc:>>"><img src="/img/editdelete.gif" name="delreq" value="<<:id_record db esc:>>" border="0" onMouseOver="overlib('Delete this request');" onMouseOut="return nd();"></a>
</td>
</tr>