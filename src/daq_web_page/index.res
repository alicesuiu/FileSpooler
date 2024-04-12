<script type="text/javascript">
    /*
     * Order the staging of an entire run (all the .root files registered for this run by DAQ)
     */
    function order(run, counter){
	objById('button_'+counter).disabled=true;

	if (<<:not_secure js:>>){
	    document.form1.action='https://alimonitor.cern.ch/DAQ/';
	    document.form1.run.value=run;
	    document.form1.submit();
	    return;
	}

        var url = "/admin/stage_run.jsp?run="+run;

        new Ajax.Request(url, {
            asynchronous : false,
            method: 'post',
            onComplete: function(transport) {

		var color = '#FFFF00';

                if(transport.status == "200"){
                    if(transport.responseText.indexOf('Error') >= 0){
                        alert("Some error occured:\n"+transport.responseText);
			color='#FF0000';
                    }
                    else{
                        bResponse = true;
                    }
                }
                else{
                    alert("HTTP error (?!?)");
		    color='#FF0000';
                }

	    	objById('td_'+counter).style.backgroundColor=color;
		objById('button_'+counter).style.display='none';
            }
        }
        );
    }

    function deleteRun(run, counter){
	objById('remove_'+counter).disabled=true;

	if (<<:not_secure js:>>){
	    document.form1.action='https://alimonitor.cern.ch/DAQ/';
	    document.form1.delete.value=run;
	    document.form1.submit();
	    return;
	}

        var url = "/admin/stage_run.jsp?delete="+run;

        new Ajax.Request(url, {
            asynchronous : false,
            method: 'post',
            onComplete: function(transport) {
		var color = '#FF9900';

                if(transport.status == "200"){
                    if(transport.responseText.indexOf('Error') >= 0){
                        alert("Some error occured:\n"+transport.responseText);
			color='#FF0000';
                    }
                    else{
                        bResponse = true;
                    }
                }
                else{
                    alert("HTTP error (?!?)");
		    color='#FF0000';
                }

	    	objById('td_'+counter).style.backgroundColor=color;
		objById('remove_'+counter).style.display='none';
            }
        }
        );
    }

    function loginAction(){
	document.form1.action='https://alimonitor.cern.ch/DAQ/';
	document.form1.submit();
    }

    function writeLogin(){
	if (<<:not_secure js:>>){
	    document.write('<a href=# onClick="return loginAction();" class=link>Login</a>');
	}
    }

    var runListCompiled = '<<:all_runs js:>>';

    var runListCompiledSpace = runListCompiled.split(',').join(' ');

    function runList(){
	showCenteredWindow(runListCompiled+'<br><br><a class=link onClick="showCenteredWindow(runListCompiledSpace, \'Space-separated run list\')">Space-separated</a>', 'Run list');
    }

</script>

<form name=form1 action=/DAQ/ method=POST>
    <input type=hidden name=run value="">
    <input type=hidden name=delete value="">

<table cellspacing=0 cellpadding=2 class="table_content">
    <tr height=25>
	<td class="table_title"><b>RAW Data Registration, Transferring and Processing</b>
	    <div align=right><script type="text/javascript">writeLogin();</script></div>
	</td>
    </tr>
    <tr>
	<td>
	    <table cellspacing=1 cellpadding=2 class=sortable>
		<thead>
		<tr height=25>
		    <td class="table_header"><b>Run#</b></td>
		    <td class="table_header"><b>Period</b></td>
		    <td class="table_header" colspan="4"><b>Global</b></td>
		    <td class="table_header" colspan="3"><b>CTF</b></td>
		    <td class="table_header" colspan="3"><b>TF</b></td>
		    <td class="table_header" colspan="3"><b>Other</b></td>
		    <td class="table_header"><b>First seen</b></td>
		    <td class="table_header"><b>Last seen</b></td>
		    <td class="table_header"><b>Replication status</b></td>
		    <td class="table_header"><b>Reconstruction status</b></td>
		    <!--
		    <td class="table_header"><b>Staging status</b></td>
		    -->
		    <td class="table_header"><b>Run length</b></td>
		    <td class="table_header"><b>Run quality</B></td>
		    <td class="table_header"><b>Action taken</b></td>
		    <td class="table_header">&nbsp;</td>
		</tr>

		<tr height=25>
			<td class="table_header">
			<input type=text name=runfilter value="<<:runfilter esc:>>" class="input_text" style="width:50px" onMouseOver="overlib('list,list,interval-interval,interval-interval');" onMouseOut="nd();">
		    </td>
		    <td class="table_header">
		    <input type=text name=partitionfilter value="<<:partitionfilter esc:>>" class="input_text" style="width:70px">
		    </td>
			<td class="table_header">
			<select name="rtype" onChange="modify();" class="input_select">
			    <option value=0 <<:rtype_0:>>>- All -</option>
			    <option value=1 <<:rtype_1:>>>raw</option>
			    <option value=2 <<:rtype_2:>>>calib</option>
			    <option value=3 <<:rtype_3:>>>other</option>
			    <option value=4 <<:rtype_4:>>>Not set</option>
			</select>
		    </td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header">
		    <input type=text name=replicaCtfFilter value="<<:replicaCtfFilter esc:>>" class="input_text" style="width:70px">
		    </td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header">
		    <input type=text name=replicaTfFilter value="<<:replicaTfFilter esc:>>" class="input_text" style="width:70px">
		    </td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header">
		    <input type=text name=replicaOtherFilter value="<<:replicaOtherFilter esc:>>" class="input_text" style="width:70px">
		    </td>
		    <td class="table_header">
			<select name=time class="input_select" onChange="modify();">
			    <option value="0" <<:time_0:>>>- All -</option>
			    <option value="1" <<:time_1:>>>Last hour</option>
			    <option value="24" <<:time_24:>>>Last day</option>
			    <option value="168" <<:time_168:>>>Last week</option>
			    <option value="720" <<:time_720:>>>Last month</option>
			    <option value="1464" <<:time_1464:>>>Last 2 months</option>
			    <option value="2190" <<:time_2190:>>>Last 3 months</option>
			    <option value="2920" <<:time_2920:>>>Last 4 months</option>
			    <option value="4320" <<:time_4320:>>>Last 6 months</option>
			    <option value="8760" <<:time_8760:>>>Last 12 months</option>
			    <option value="17520" <<:time_17520:>>>Last 24 months</option>
			    <option value="26304" <<:time_26304:>>>Last 36 months</option>
			    <option value="100000" <<:time_100000:>>>Forever</option>
			</select>
		    </td>
		    <td class="table_header"></td>
		    <td class="table_header">
			<select name="transfer" onChange="modify();" class="input_select">
			    <option value=0 <<:transfer_0:>>>- All -</option>
			    <option value=2 <<:transfer_2:>>>Started</option>
			    <option value=3 <<:transfer_3:>>>Completed</option>
			    <option value=1 <<:transfer_1:>>>Not transferred</option>
			</select>
		    </td>
		    <td class="table_header">
			<select name="processing" onChange="modify();" class="input_select">
			    <option value=0 <<:processing_0:>>>- All -</option>
			    <option value=1 <<:processing_1:>>>Started</option>
			    <option value=2 <<:processing_2:>>>Completed</option>
			    <option value=3 <<:processing_3:>>>Not considered</option>
			</select>
		    </td>
		    <td class="table_header">
			<select name=runlength onChange="modify();" class="input_select">
			    <option value=0 <<:runlength_0:>>>- All -</option>
			    <option value=1 <<:runlength_1:>>>Less than 5min</option>
			    <option value=2 <<:runlength_2:>>>More than 5min</option>
			</select>
		    </td>
		    <td class="table_header">
			<select name=goodrun onChange="modify();" class="input_select">
			    <option value=0 <<:goodrun_0:>>>- All -</option>
			    <option value=1 <<:goodrun_1:>>>Good run</option>
			    <option value=2 <<:goodrun_2:>>>Bad run</option>
			    <option value=3 <<:goodrun_3:>>>Not set</option>
			    <option value=4 <<:goodrun_4:>>>Test run</option>
			</select>
		    </td>
		    <td class="table_header">
			<select name="raction" onChange="modify();" class="input_select">
			    <option value=0 <<:action_0:>>>- All -</option>
			    <option value=1 <<:action_1:>>>Copy</option>
			    <option value=2 <<:action_2:>>>Move</option>
			    <option value=3 <<:action_3:>>>Delete</option>
			    <option value=4 <<:action_4:>>>Not set</option>
			    <option value=5 <<:action_5:>>>Delete replica</option>
			    <option value=6 <<:action_6:>>>Not deleted yet</option>
			</select>
		    </td>
		    <td class=table_header>
			<input type=submit name=submitter class=input_submit value="&raquo;">
		    </td>
		</tr>

		<tr height=25>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"><b>Type</b></td>
		    <td class="table_header"><b>Chunks</b></td>
		    <td class="table_header"><b>Avg file size</b></td>
		    <td class="table_header"><b>Total size</b></td>
		    <td class="table_header"><b>Count</b></td>
		    <td class="table_header"><b>Size</b></td>
		    <td class="table_header"><b>Replica</b></td>
		    <td class="table_header"><b>Count</b></td>
		    <td class="table_header"><b>Size</b></td>
		    <td class="table_header"><b>Replica</b></td>
		    <td class="table_header"><b>Count</b></td>
		    <td class="table_header"><b>Size</b></td>
		    <td class="table_header"><b>Replica</b></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <!--
		    <td class="table_header">
			<select name=staging onChange="modify();" class="input_select">
			    <option value=0 <<:staging_0:>>>- All -</option>
			    <option value=1 <<:staging_1:>>>Staged</option>
			    <option value=2 <<:staging_2:>>>Scheduled</option>
			    <option value=3 <<:staging_3:>>>Unstaged</option>
			</select>
		    </td>
		    -->
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		    <td class="table_header"></td>
		</tr>
		</thead>
		<tbody>
		<<:content:>>
		</tbody>
		<tfoot>
		<tr height=25>
		    <td align=right class="table_header"><span onClick="runList()"><<:runs esc:>> runs</span></td>
		    <td class="table_header">&nbsp;</td>
		    <td class="table_header"></td>
		    <td align=right class="table_header"><<:files esc:>> files</td>
		    <td align=right class="table_header"><<:average_file_size size:>></td>
		    <td align=right class="table_header"><<:totalsize size:>></td>
		    <td align=right class="table_header"><<:total_ctf_count esc:>></td>
		    <td align=right class="table_header"><<:total_ctf_size size:>></td>
		    <td class="table_header">&nbsp;</td>
		    <td align=right class="table_header"><<:total_tf_count esc:>></td>
		    <td align=right class="table_header"><<:total_tf_size size:>></td>
		    <td class="table_header">&nbsp</td>
		    <td align=right class="table_header"><<:total_other_count esc:>></td>
		    <td align=right class="table_header"><<:total_other_size size:>></td>
 		    <td class="table_header">&nbsp;</td>
		    <td class="table_header">&nbsp;</td>
		    <td class="table_header">&nbsp;</td>
		    <td class="table_header">
			<table border=0 cellspacing=0 cellpadding=0 width=100%>
			    <tr>
				<td bgcolor=#FFFFFF width=33% onMouseOver="overlib('Waiting transfers: <<:transfers_unknown esc:>>');" onMouseOut="nd();"><<:transfers_unknown esc:>></td>
				<td bgcolor=#FFFF00 width=33% onMouseOver="overlib('Scheduled transfers: <<:transfers_scheduled esc:>>');" onMouseOut="nd();"><<:transfers_scheduled esc:>></td>
				<td bgcolor=#00FF00 width=33% onMouseOver="overlib('Completed transfers: <<:transfers_completed esc:>>');" onMouseOut="nd();"><<:transfers_completed esc:>></td>
			    </tr>
			</table>
		    </td>
		    <td class="table_header">
		    	<table border=0 cellspacing=0 cellpadding=0 width=100%>
			    <tr>
				<td bgcolor=#FFFFFF width=33% onMouseOver="overlib('Waiting jobs: <<:jobs_unknown esc:>>');" onMouseOut="nd();"><<:jobs_unknown esc:>></td>
				<td bgcolor=#FFFF00 width=33% onMouseOver="overlib('Started jobs: <<:jobs_started esc:>>');" onMouseOut="nd();"><<:jobs_started esc:>></td>
				<td bgcolor=#00FF00 width=33% onMouseOver="overlib('Completed jobs: <<:jobs_completed esc:>>');" onMouseOut="nd();"><<:jobs_completed esc:>></td>
			    </tr>
			</table>
		    </td>
		    <!--
		    <td class="table_header">
		    	<table border=0 cellspacing=0 cellpadding=0 width=100%>
			    <tr>
				<td bgcolor=#FFFFFF width=33% onMouseOver="overlib('Unstaged runs: <<:staged_unknown esc:>>');" onMouseOut="nd();"><<:staged_unknown esc:>></td>
				<td bgcolor=#FFFF00 width=33% onMouseOver="overlib('Staging scheduled: <<:staged_started esc:>>');" onMouseOut="nd();"><<:staged_started esc:>></td>
				<td bgcolor=#00FF00 width=33% onMouseOver="overlib('Staging ordered: <<:staged_completed esc:>>');" onMouseOut="nd();"><<:staged_completed esc:>></td>
			    </tr>
			</table>
		    </td>
		    -->
			<td align=right class="table_header" onMouseOver="overlib('Good: <<:good_runs_duration interval:>> / Bad: <<:bad_runs_duration interval:>> / Test: <<:test_runs_duration interval:>>');" onMouseOut="nd();"><<:runs_duration interval:>></td>
		    <td class="table_header">
		    	<table border=0 cellspacing=0 cellpadding=0 width=100%>
			    <tr>
				<td bgcolor=#7FFF00 width=33% onMouseOver="overlib('Good runs: <<:good_runs esc:>>');" onMouseOut="nd();"><<:good_runs esc:>></td>
				<td bgcolor=#FA8072 width=33% onMouseOver="overlib('Bad runs: <<:bad_runs esc:>>');" onMouseOut="nd();"><<:bad_runs esc:>></td>
				<td bgcolor=#FAFAD2 width=33% onMouseOver="overlib('Test runs: <<:test_runs esc:>>');" onMouseOut="nd();"><<:test_runs esc:>></td>
			    </tr>
			</table>
		    </td>
		    <td class="table_header">&nbsp;</td>
		    <td class="table_header">&nbsp;</td>
		    <td class="table_header">&nbsp;</td>
		</tr>
		</tfoot>
	    </table>
	</td>
    </tr>
</table>

</form>
