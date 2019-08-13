using System;
using System.Collections.Generic;
using System.IO;

public class reglamentGATE
{
    public static bool send_pids_to_eir(ref List<clsConnections> link_connections, int wait_interval, ref string comment)
    {
        bool result = false;
        comment = string.Empty;
        List<string[]> list = new List<string[]>();
        try
        {
            if (clsLibrary.execQuery_getListString(
                ref list, ref link_connections, "server-r", "srz3_00"
                , string.Format(
                    "select id pid, dr, pid doubler, q smocode, lpu mcode, ss_doctor doctor1, ss_feldsher doctor2, dedit, RSTOP, tmpForSRZ.dbo.GATE_CheckActivePID_date(ID,'{0}') active " +
                    "FROM  people where DEDIT >= '{1}'",
                    DateTime.Now.ToString("yyyy-MM-dd"),
                    clsLibrary.execQuery_PGR_getString(ref link_connections, "postgres", "select to_char(coalesce(max(dedit)- interval '1 hour','1900-01-01')::date,'YYYY-MM-DD') from identy.pids;")
                    ),
                wait_interval
                ))
            {
                //Работаем со списком
                if (list.Count > 0)
                {
                    comment = list.Count.ToString();
                    if (clsLibrary.execQuery_PGR(ref link_connections, "postgres",
                                    "drop table if exists identy.tmppids; " +
                                    "create table identy.tmppids( " +
                                    "pid int8 NOT NULL, " +
                                    "dr timestamp NULL, " +
                                    "doubler int8 NULL, " +
                                    "smocode varchar(5) NULL, " +
                                    "mcode varchar(6) NULL, " +
                                    "doctor1 varchar(14) NULL, " +
                                    "doctor2 varchar NULL, " +
                                    "fias_aoid varchar(36) NULL, " +
                                    "fias_houseid varchar(36) NULL, " +
                                    "pfias_aoid varchar(36) NULL, " +
                                    "pfias_houseid varchar(36) NULL, " +
                                    "fias_aoguid varchar(36) NULL, " +
                                    "fias_houseguid varchar(36) NULL, " +
                                    "pfias_aoguid varchar(36) NULL, " +
                                    "pfias_houseguid varchar(36) NULL, " +
                                    "dedit timestamp NULL, " +
                                    "rstop numeric NULL, " +
                                    "active numeric NULL); "))
                    {
                        if (clsLibrary.execQuery_PGR_insertList(ref link_connections, "postgres",
                            "insert into identy.tmppids (pid, dr, doubler, smocode, mcode, doctor1, doctor2, dedit, rstop, active) values ",
                            ref list, 1000))
                        {
                            result = clsLibrary.execQuery_PGR(ref link_connections, "postgres",
                                "with list as " +
                                "(select tmp.pid _pid, tmp.dr _dr, tmp.doubler _doubler, tmp.smocode _smocode, tmp.mcode _mcode, tmp.doctor1 _doctor1, tmp.doctor2 _doctor2, tmp.dedit _dedit, tmp.rstop _rstop, tmp.active _active  from identy.tmppids tmp " +
                                 "join identy.pids prod on prod.pid = tmp.pid) " +
                                "update identy.pids set " +
                                "dr = list._dr, doubler = list._doubler, smocode = list._smocode, mcode = list._mcode, doctor1 = list._doctor1, " +
                                "doctor2 = list._doctor2, dedit = list._dedit, rstop = list._rstop, active = list._active " +
                                "from list " +
                                "where pid = list._pid; " +

                                "with list as " +
                                "(select tmp.* from identy.tmppids tmp " +
                                "left outer join identy.pids prod on prod.pid = tmp.pid " +
                                "where prod.pid is null) " +
                                "insert into identy.pids(pid, dr, doubler, smocode, mcode, doctor1, doctor2, dedit, rstop, active) " +
                                "select pid, dr, doubler, smocode, mcode, doctor1, doctor2, dedit, rstop, active from list; " +
                                "drop table if exists identy.tmppids; ", 1000000);
                            if(!result)
                                comment = "ошибка вставки в продуктивную базу";
                        }
                        else
                            comment = "ошибка вставки во временную таблицу";
                    }
                    else
                        comment = "ошибка создания временной таблицы";
                }
                else
                {
                    result = true;
                    comment = "нет данных";
                }
            }
            else
                comment = "не получены данные СРЗ";               
        }
        catch(Exception e)
        {
            comment = e.Message;
        }
        finally
        {
            list = null;
            GC.Collect();
        }
        return result;
    }

    // Не хроший вариант сборкм XML простыми текстовыми строками
    public static bool send_response_from_eir(ref List<clsConnections> link_connections, int wait_interval, ref string comment)
    {
        bool result = false;
        comment = string.Empty;
        List<string[]> responses = new List<string[]>();
        List<string[]> rows = new List<string[]>();
        List<string> content = new List<string>();
        try
        {
            comment += " get responses";
            result = clsLibrary.ExecQurey_PGR_GetListStrings(
                    ref link_connections, null, "postgres"
                    , "select id, header, filename from buf_eir.response where state::int >= 3 and schema_name = 'zldn_schema_smo_2_0' and date_send is null and " +
                        "schema_name in (select unnest(schemas_table) schema_name from buf_eir.config_tables where 'RESPONSE' = any (events)) order by \"order\" limit 10;"
                    , ref responses
            );
            if(result)
            {
                comment += " " + responses.Count.ToString();
                foreach (string[] response in responses)
                {
                    content.Clear();
                    content.Add("<?xml version=\"1.0\" encoding=\"windows-1251\"?>");
                    content.Add("<ZLDN>");
                    content.Add(response[1]);
                    rows.Clear(); 
                    comment += " get rows " + response[0];
                    result = clsLibrary.ExecQurey_PGR_GetListStrings(
                        ref link_connections, null, "postgres"
                        , string.Format("select gpid, personalinfo, content from buf_eir.response_content where id_response = '{0}' order by id;", response[0])
                        , ref rows
                    );
                    if (result)
                    {
                        foreach (string[] row in rows)
                        {
                            comment += " get row";
                            content.Add("<ZL>");
                            content.Add("<GPID>" + row[0] + "</GPID>");
                            content.Add(row[1]);
                            content.Add(row[2]);
                            content.Add("</ZL>");
                        }
                        content.Add("</ZLDN>");
                        result = clsLibrary.createFileTXT_FromList(content, Path.Combine(@"W:\", "test_" + response[2]));

                        clsLibrary.execQuery_PGR_function_bool(ref link_connections, "postgres"
                           , String.Format("update buf_eir.response set state = 100, date_send = '{0}' where id = '{1}';", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), response[0])
                           , 120000);
                    }
                        
                }
            }            
        }
        catch (Exception e)
        {
            comment += e.Message;
        }
        finally
        {
            responses = null;
            rows = null;
            content = null;
            GC.Collect();
        }
        return result;
    }
}

