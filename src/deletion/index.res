<script type="text/javascript">
    var runListCompiled = '<<:all_runs js:>>';

    var runListCompiledSpace = runListCompiled.split(',').join(' ');

    function runList() {
        showCenteredWindow(runListCompiled+'<br><br><a class=link onClick="showCenteredWindow(runListCompiledSpace, \'Space-separated run list\')">Space-separated</a>', 'Run list');
    }

    function checkAll(master) {
        var chk = master.checked;

        var chkboxes = document.getElementsByName("bulk_del");
        for (i=0; i< chkboxes.length; i++)
            chkboxes[i].checked = chk;
    }

    function showAddForm(){
        showCenteredWindowSize('<iframe src="/admin/deletion/add.jsp" border=0 width=100% height=100% frameborder="0" marginwidth="0" marginheight="0" scrolling="yes" align="absmiddle" vspace="0" hspace="0"></iframe>', 'Add new deletion request', 800, 500);
    }

    function deleteAll() {
        var url = "https://alimonitor.cern.ch/admin/deletion/delete.jsp";
        var runs = "";
        var chkboxes = document.getElementsByName("bulk_del");
        for (i=0; i< chkboxes.length; i++) {
            if (chkboxes[i].checked) {
                runs += chkboxes[i].value + " ";
            }
        }

        if (runs.length > 0)
            url += "?runs=" + runs;
        window.location.href=url;
    }
</script>

<form name=form1 action=/deletion/ method=POST>
<table cellspacing=0 cellpadding=2 class="table_content">
    <tr height=25>
    <td class="table_title"><b>RAW Data Deletion Actions (<a class=link href="javascript:showAddForm()">add new request</a>)</b>
        <div align=right>
        <<:com_authenticated_start:>>
            <a onMouseOver="overlib('Access to administrator functions');" onMouseOut="return nd();" href="https://alimonitor.cern.ch/deletion/index.jsp"><b>Login</b>
        <<:com_authenticated_end:>>
        </div>
    </td>
    </tr>
    <tr>
    <td>
        <table cellspacing=1 cellpadding=2 class=sortable>
        <thead>
        <tr height=25>
            <td class="table_header"><b>Run</b></td>
            <td class="table_header"><b>Action</b></td>
            <td class="table_header"><b>Type</b></td>
            <td class="table_header"><b>Total Chunks</b></td>
            <td class="table_header"><b>Total Size</b></td>
            <td class="table_header"><b>Chunks to delete</b></td>
            <td class="table_header"><b>Size to delete</b></td>
            <td class="table_header"><b>Percentage</b></td>
            <td class="table_header"><b>Storage</b></td>
            <td class="table_header"><b>Source</b></td>
            <td class="table_header"><b>Status</B></td>
            <td class="table_header"><b>Addtime</B></td>
            <td class="table_header">
            <<:com_admin_start:>><b>Option</b><<:com_admin_end:>>
            </td>
            <td class="table_header">&nbsp;</td>
        </tr>

        <tr height=25>
            <td class="table_header">
                <input type=text name=runfilter value="<<:runfilter esc:>>" class="input_text" style="width:50px">
            </td>
            <td class="table_header">
                <select name="action" onChange="modify();" class="input_select">
                    <option value=0 <<:action_0:>>>- All -</option>
                    <option value=1 <<:action_1:>>>delete</option>
                    <option value=2 <<:action_2:>>>delete replica</option>
                </select>
            </td>
            <td class="table_header">
                <input type=text name=typefilter value="<<:typefilter esc:>>" class="input_text" style="width:50px">
            </td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header"></td>
            <td class="table_header">
                <input type=text name=storagefilter value="<<:storagefilter esc:>>" class="input_text" style="width:140px">
            </td>
            <td class="table_header">
                <input type=text name=sourcefilter value="<<:sourcefilter esc:>>" class="input_text" style="width:70px">
            </td>
            <td class="table_header">
                <select name="status" onChange="modify();" class="input_select">
                    <option value=0 <<:status_0:>>>- All -</option>
                    <option value=1 <<:status_1:>>>Queued</option>
                    <option value=2 <<:status_2:>>>In progress</option>
                </select>
            </td>
            <td class="table_header"></td>
            <td class="table_header" nowrap>
            <<:com_admin_start:>>
               <input type=checkbox id="check_all" name="check_all" onMouseOver="overlib('Select all (visible) runs for deletion');" onMouseOut="nd();" onClick="checkAll(this);">
            <<:com_admin_end:>>
            </td>
            <td class="table_header"><input type=submit name=submit value="&raquo;" class="input_submit"></td>
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
            <td align=right class="table_header"><<:files esc:>> files</td>
            <td align=right class="table_header"><<:totalsize size:>></td>
            <td align=right class="table_header"><<:files_to_delete esc:>> files</td>
            <td align=right class="table_header"><<:totalsize_to_delete size:>></td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header">&nbsp;</td>
            <td class="table_header" nowrap>
            <<:com_admin_start:>>
                <input type="button" class="input_submit" value="Delete" onMouseOver="overlib('Delete selected runs');" onMouseOut="return nd();" onClick="deleteAll();"><br>
            <<:com_admin_end:>>
            </td>
            <td class="table_header">&nbsp;</td>
        </tr>
        </tfoot>
        </table>
    </td>
    </tr>
</table>

</form>