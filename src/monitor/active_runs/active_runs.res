<form name=form1 action=active_runs.jsp method=get>
<table cellspacing=0 cellpadding=2 class="table_content">
    <tr height=35>
	<td class="table_title">
	    <table border=0 cellspacing=0 cellpadding=0 width=100%>
		<tr>
		    <td align=center>
			<b>EPN2EOS - List of runs that are transferred to storage</b>
		    </td>
		</tr>
	    </table>
	</td>
    </tr>
    <tr>
	<td>
	    <table cellspacing=1 cellpadding=2 class=sortable>
		<thead>
		<tr height=50>
            <th class="table_header_stats"></th>
            <th class="table_header_stats" colspan=5>Pending data to be transferred (on EPN local disks)<br>Current aggregated transfer speed: <font color=green> <<:global_rate size:>>/s</font></th>
            <th class="table_header_stats" colspan=1>Transfer delay</th>
            <th class="table_header_stats" colspan=3>Data transferred so far</th>
        </tr>
		<tr height=50>
		    <th class="table_header_stats" width=80>Run</th>
            <th class="table_header_stats" width=80>Files to be transferred</th>
            <th class="table_header_stats" width=120>Total size of files to be transferred</th>
            <th class="table_header_stats" width=120>Total number of EPNs involved in transfer</th>
            <th class="table_header_stats" width=80>ETA</th>
            <th class="table_header_stats" width=80>Last transfer timestamp</th>

            <th class="table_header_stats" width=130><b>Last file registered - last file written</b></th>

            <th class="table_header_stats" width=80><b>Files registered</b></th>
            <th class="table_header_stats" width=80><b>Total size</b></th>
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
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"></th>
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_eta intervals:>></th>
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"></th>
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"></th>
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_registered_files dot:>></th>
            <th nowrap align=right class="table_header_stats" style="background-color: #E3ECFF"><<:total_registered_size size:>></th>
		</tr>
		</tfoot>

	    </table>
	</td>
    </tr>
</table>
</form>
