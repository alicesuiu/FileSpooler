<form name=form1 action=active_runs.jsp method=get>
<table cellspacing=0 cellpadding=2 class="table_content">
    <tr height=25>
	<td class="table_title">
	    <table border=0 cellspacing=0 cellpadding=0 width=100%>
		<tr>
		    <td align=center>
			<b>EPN2EOS - List of active runs</b>
		    </td>
		</tr>
	    </table>
	</td>
    </tr>
    <tr>
	<td>
	    <table cellspacing=1 cellpadding=2 class=sortable>
		<thead>
		<tr height=25>
		    <th class="table_header_stats" width=80><b>Run</b></th>
		    <th class="table_header_stats" width=80><b>Files</b></th>
		    <th class="table_header_stats" width=80><b>Total size</b></th>
		    <th class="table_header_stats" width=80><b>ETA</b></th>
		    <th class="table_header_stats" width=80><b>Last Seen</b></th>
		</tr>
		</thead>

		<tbody>
		<<:content:>>
		</tbody>

		<tfoot>
		<tr>
		    <th align=left class="table_header_stats" nowrap style="background-color: #C0D5FF">Total: <<:total_runs:>> runs</th>
		    <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_files dot:>></th>
		    <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_size size:>></th>
		    <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_eta intervals:>></th>
		</tr>
		</tfoot>

	    </table>
	</td>
    </tr>
</table>
</form>
