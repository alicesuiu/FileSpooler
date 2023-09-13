<tr class="table_row" height=27 >
<td align=right class="table_row" sorttable_customkey="<<:run db:>>"><a href="/DAQ/?time=0&runfilter=<<:run db enc:>>"><<:run db esc:>></a></td>
<td align=right class="table_row" sorttable_customkey="<<:action db:>>"><<:action db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:filter db:>>"><<:filter db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:counter db:>>"><<:counter db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:size db:>>"><<:size db size:>></td>
<td align=right class="table_row" sorttable_customkey="<<:sourcese db:>>"><<:sourcese db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:source db:>>"><<:source db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:status db:>>"><<:status db esc:>></td>
<td align=right class="table_row" sorttable_customkey="<<:addtime db:>>"><<:addtime db nicedate:>></td>
<td align=right class="table_row">
	<<:com_authenticated_start:>>
		<input type="checkbox" name="bulk_del" value="<<:run esc:>>" onMouseOver="overlib('Mark run <<:run esc js:>> for deletion')" onMouseOut="nd();">
	<<:com_authenticated_end:>>
</td>
</tr>