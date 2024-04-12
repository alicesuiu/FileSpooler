<script type="text/javascript">
    var runListCompiled = '<<:all_runs js:>>';

    var runListCompiledSpace = runListCompiled.split(',').join(' ');

    function runList() {
        showCenteredWindow(runListCompiled+'<br><br><a class=link onClick="showCenteredWindow(runListCompiledSpace, \'Space-separated run list\')">Space-separated</a>', 'Run list');
    }

    function submitForm(page) {
        document.form1.p.value = page;
        var formSubmitEvent = document.createEvent('Event');
        formSubmitEvent.initEvent('submit', true, true);
        document.form1.dispatchEvent(formSubmitEvent);
    }
</script>

<form name=form1 action=/deletion/taken.jsp method=POST>
<table cellspacing=0 cellpadding=2 class="table_content">
    <tr height=25>
    <td class="table_title"><b>RAW Data Already Removed</b>
        <div align=right>
        <<:!com_authenticated_start:>>
            <a onMouseOver="overlib('Access to administrator functions');" onMouseOut="return nd();" href="https://alimonitor.cern.ch/deletion/taken.jsp"><b>Login</b>
        <<:!com_authenticated_end:>>
        <<:com_authenticated_start:>>

        <<:com_authenticated_end:>>
            Welcome <b><<:account esc:>></b>
        <<:com_authenticated_end:>>
        </div>
    </td>
    </tr>
    <tr>
    <td>
        <table cellspacing=1 cellpadding=2 class=sortable>
        <thead>

        <tr height=25>
            <td class="table_header">
                <input type=text name=runfilter value="<<:runfilter esc:>>" class="input_text" style="width:50px">
            </td>
            <td class="table_header">
                <input type=text name=runpartition value="<<:runpartition esc:>>" class="input_text" style="width:50px">
            </td>
            <td class="table_header">
                <select name="action" onChange="modify();" class="input_select">
                    <option value=0 <<:action_0:>>>- All -</option>
                    <option value=1 <<:action_1:>>>delete</option>
                    <option value=2 <<:action_2:>>>delete replica</option>
                </select>
            </td>
            <td class="table_header">
                <input type=text name=storagefilter value="<<:storagefilter esc:>>" class="input_text" style="width:140px">
            </td>
            <td class="table_header">
                <select name="datafilter" onChange="modify();" class="input_select">
                    <option value=0 <<:datafilter_0:>>>- All -</option>
                    <option value=1 <<:datafilter_1:>>>TF</option>
                    <option value=2 <<:datafilter_2:>>>CTF</option>
                    <option value=3 <<:datafilter_3:>>>Other</option>
                </select>
            </td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header">
                <input type=text name=reqfilter value="<<:reqfilter esc:>>" class="input_text" style="width:70px">
            </td>
            <td class="table_header">
                <input type=text name=respfilter value="<<:respfilter esc:>>" class="input_text" style="width:70px">
            </td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header"><input type=submit name=submit value="&raquo;" class="input_submit"></td>
        </tr>

        <tr height=25>
            <td class="table_header"><b>Run</b></td>
            <td class="table_header"><b>Period</b></td>
            <td class="table_header"><b>Action</b></td>
            <td class="table_header"><b>Storage</b></td>
            <td class="table_header"><b>Data filter</b></td>
            <td class="table_header"><b>Deleted files</b></td>
            <td class="table_header"><b>Deleted size</b></td>
            <td class="table_header"><b>Requester</b></td>
            <td class="table_header"><b>Responsible</b></td>
            <td class="table_header"><b>Reason</b></td>
            <td class="table_header"><b>Date of removal</b></td>
            <td class="table_header">&nbsp;</td>
        </tr>
        </thead>

        <tbody>
        <<:content:>>
        </tbody>

        <tfoot>
        <tr height=25>
            <td align=right class="table_header"><span onClick="runList()"><<:runs esc:>> runs</span></td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td align=right class="table_header"><<:deleted_files esc:>> files</td>
            <td align=right class="table_header"><<:deleted_size size:>></td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
        </tr>
        </tfoot>
        </table>
    </td>
    </tr>

    <input type=hidden name=p value="0">
    <tr>
        <td>
            <table border=0 cellspacing=0 cellpadding=0 width=100%>
            <tr>
                <td width=33% align=left>
                    <<:com_prev_start:>>
                        <a href="javascript:void(0)" onClick="submitForm('<<:prev_page js esc:>>'); return false;" class=link>&laquo; Prev page &laquo;</a>
                    <<:com_prev_end:>>
                </td>
                <td width=33% align=center>Requests per page:
                    <select name="limit" class="input_select" onChange="modify();">
                        <option <<:limit_100:>> value=100>100</option>
                        <option <<:limit_500:>> value=500>500</option>
                        <option <<:limit_1000:>> value=1000>1000</option>
                        <option <<:limit_-1:>> value=-1>- All -</option>
                    </select>
                </td>
                <td width=33% align=right>
                    <<:com_next_start:>>
                        <a href="javascript:void(0)" onClick="submitForm('<<:next_page js esc:>>'); return false;" class=link>&raquo; Next page &raquo;</a>
                    <<:com_next_end:>>
                </td>
            </tr>
            </table>
        </td>
    </tr>
</table>
</form>