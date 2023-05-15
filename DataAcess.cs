using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace scrollproject
{
    public class DataAcess
    {

        private OracleConnection conn = null;
        public DataAcess()
        {
            var connectionstring = ConfigurationManager.ConnectionStrings["test"].ConnectionString;
            try
            {
                Log.Trace("データベース接続が開始されました...");
                conn = new OracleConnection(connectionstring);

                conn.Open();
                Log.Trace("データベースが接続されています。");
            }
            catch (Exception ex)
            {
                Log.Error($"{connectionstring} 存在しません。: {ex}");
            }
}

        public void TruncateFileRecordInsertedTable()
        {
            try
            {
                Log.Trace("テーブルの開始を切り捨てる");
                var trunsql = "TRUNCATE TABLE KMTT01FL01_CSVData";
                OracleCommand cmd = conn.CreateCommand();
                cmd.CommandText = trunsql;
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
                Log.Trace("切り捨てられたテーブル ");
            }
            catch (Exception ex)
            {
                Log.Error($"{ex}");
            }
        }

        public void UploadFile(string filloaction)
        {
            Log.Trace("ファイルのロードを開始しています ...");

            if (!File.Exists(filloaction))
            {
                throw new FileNotFoundException("入力ファイルが指定されたパスに存在しません", filloaction);
            }

            using (StreamReader reader = new StreamReader(filloaction))
            {

                var SEIKYOUCD = new List<int>();
                var SHISYO = new List<int>();
                var CSYOUBI = new List<int>();
                var AP = new List<string>();
                var CSNO = new List<int>();
                var HAISOU = new List<int>();
                var HANCD = new List<int>();
                var KUMIAIINCD = new List<int>();
                var JYUCYUU = new List<int>();
                var CHUNO = new List<int>();
                var SUURYOU = new List<int>();
                var COUNTNO = new List<int>();

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var cols = line.Split(',');

                    SEIKYOUCD.Add(Convert.ToInt32(cols[0]));
                    SHISYO.Add(Convert.ToInt32(cols[1]));
                    CSYOUBI.Add(Convert.ToInt32(cols[2]));
                    AP.Add(cols[3]);
                    CSNO.Add(Convert.ToInt32(cols[4]));
                    HAISOU.Add(Convert.ToInt32(cols[5]));
                    HANCD.Add(Convert.ToInt32(cols[6]));
                    KUMIAIINCD.Add(Convert.ToInt32(cols[7]));
                    JYUCYUU.Add(Convert.ToInt32(cols[8]));
                    CHUNO.Add(Convert.ToInt32(cols[9]));
                    SUURYOU.Add(Convert.ToInt32(cols[10]));
                    COUNTNO.Add(Convert.ToInt32(cols[11]));
                }

                OracleCommand cmd = conn.CreateCommand();

                cmd.CommandText = "INSERT INTO test(SEIKYOUCD,SHISYO,CSYOUBI,AP,CSNO,HAISOU,HANCD,KUMIAIINCD,JYUCYUU,CHUNO,SUURYOU,COUNTNO) " +
                    "VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)";
                cmd.ArrayBindCount = SEIKYOUCD.Count;

                OracleParameter param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = SEIKYOUCD.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = SHISYO.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = CSYOUBI.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Varchar2;
                param.Value = AP.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = CSNO.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = HAISOU.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = HANCD.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = KUMIAIINCD.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = JYUCYUU.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = CHUNO.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = SUURYOU.ToArray();
                cmd.Parameters.Add(param);

                param = new OracleParameter();
                param.OracleDbType = OracleDbType.Int32;
                param.Value = COUNTNO.ToArray();
                cmd.Parameters.Add(param);

                cmd.ExecuteNonQuery();

                Log.Trace("ファイル データが一時テーブルに正常に読み込まれました.");

                //while (!reader.EndOfStream)
                //{
                //    var line = reader.ReadLine();
                //    var values = line.Split(',');
                //    var sql = $"INSERT INTO KMTT01FL01_CSVData(SEIKYOUCD,SHISYO ,CSYOUBI,AP,CSNO,HAISOU ,HANCD,KUMIAIINCD ,JYUCYUU ,CHUNO ,SUURYOU ,COUNTNO) " +
                //        $"VALUES({values[0]},{values[1]},{values[2]},'{values[3]}',{values[4]},{values[5]},{values[6]},{values[7]},{values[8]},{values[9]},{values[10]},{values[11]})";
                //    //var sql = $"INSERT INTO KMTT01FL01_CSVData VALUES('{values[0]}'";
                //    //for (int i = 1; i < values.Length; ++i)
                //    //    sql += $",'{values[i]}'";
                //    //sql += ")"; 

                //    OracleCommand cmd = conn.CreateCommand();
                //    cmd.CommandText = sql;
                //    cmd.CommandType = CommandType.Text;
                //    cmd.ExecuteNonQuery();
                //}
            }

        }

        

        public void DeleteExistingData()
        {
            Log.Trace("テーブルからの重複データの削除.");
            //var loctablequery = @"LOCK TABLE KMTT01FL01_sampletest IN EXCLUSIVE MODE NOWAIT";
            var deletequery = @"
                 DELETE  FROM  test c  
                 WHERE EXISTS (SELECT * FROM test a
                 JOIN test b 
                 ON  a.SEIKYOUCD = b.COOPCD
                 WHERE b.KBN = 1 AND b.COOPKBN = c.COOPKBN AND 
                 a.KUMIAIINCD = c.KUMICD AND a.CHUNO = c.CHUNO)";

            //using (var cmd = new OracleCommand(loctablequery, conn))
            //{
            //    cmd.ExecuteNonQuery();
            //}

            using (var cmd = new OracleCommand(deletequery, conn))
            {
                cmd.ExecuteNonQuery();
            }
            Log.Trace("重複データが削除されました。");
        }


        public void GetKosinId(out int COOPKBN, out int UserId, out int PostCd)
        {
            COOPKBN = int.Parse(ConfigurationManager.AppSettings["COOPKBN"]);
            UserId = int.Parse(ConfigurationManager.AppSettings["User"]);
            PostCd = 0;
            var query = $"SELECT NVL(POSTCD, 0) AS POSTCD FROM MS_USER " +
                $"WHERE COOPKBN={COOPKBN} AND USERCD={UserId}";
            using (OracleCommand cmd = conn.CreateCommand())
            { 
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && reader.HasRows)
                    {
                        PostCd = reader.GetInt32(0);
                    }
                    else
                    {
                        throw new InvalidDataException("Invalid COOPKBN or USERCD");
                    }
                }
            }
        }
        public void UploadDataToFinalTable()
        {
            Log.Trace("データは最終テーブルに読み込まれています。");

            var insertsql = @"
INSERT INTO  test (
    COOPKBN,
    SISYOCD,
    CRSCD,
    CRSJUNI,
    HANCD,
    HANNMK,
    HANNMN,
    KUMICD,
    KNAMEK,
    KNAMEN,
    POSTNO,
    JYUSYON,
    TELNO,
    CHUNO,
    SURYO,
    BOOKNO,
    JUCHUKAI,
    KOSINCNT,
    KOSINCOOPKBN,
    KOSINSISYOCD,
    KOSINID,
    KOSINYMD,
    KOSINTIME) 
SELECT  b.COOPKBN,
        a.SHISYO,
        a.CSYOUBI || a.AP || a.CSNO AS CRSCD,
        a.HAISOU, 
        a.HANCD,
        NVL(c.HANNMN,f.KANMEI) AS HANNMN,
        NVL(c.HANNMX,f.KAZMEI) AS HANNMX,
        a.KUMIAIINCD, 
        NVL(d.KUMINMX,e.KANANAM) AS KUMINMX,
        NVL(d.KUMINMN,e.KAJINAM) AS  KUMINMN,
        NVL(d.POSTNO,e.POSTNO1) AS POSTNO, 
        NVL(JYUSYO1N || JYUSYO2N,e.KANJI11 || e.KANJI12) AS JYUSYON,
        NVL(d.TELNO,e.TEL1) AS TELNO,
        a.CHUNO, 
        a.SUURYOU, 
        a.COUNTNO,
        a.JYUCYUU, 
        1 AS KOSINCNT,
        {0} AS KOSINCOOPKBN,
        {1} AS KOSINSISYOCD,
        {2} AS KOSINID,
        TO_CHAR(SYSDATE,'DDMMYYYY') AS KOSINYMD,
        TO_CHAR(SYSDATE,'HH24MISS') AS KOSINTIME 
FROM  test a
JOIN test.test b
ON  a.SEIKYOUCD = b.COOPCD
LEFT JOIN test c 
ON b.COOPKBN = c.COOPKBN AND a.HANCD = c.HANCD
LEFT JOIN test f 
ON  b.COOPKBN = f.COOPKBN AND a.HANCD = f.HANCD
LEFT JOIN test d 
ON  b.COOPKBN = d.COOPKBN AND d.KUMICD = a.KUMIAIINCD
LEFT JOIN test e
ON e.COOPKBN = b.COOPKBN AND e.KOJINCD = a.KUMIAIINCD
WHERE b.KBN = 1";

            int COOPKBN, POSTCD, USERID;
            GetKosinId(out COOPKBN, out USERID, out POSTCD);

            OracleCommand cmd = conn.CreateCommand();
            cmd.CommandText = string.Format(insertsql, COOPKBN, POSTCD, USERID);
            cmd.CommandType = CommandType.Text;
            cmd.ExecuteNonQuery();

            Log.Trace("データが正常に読み込まれました");


        }

    public void BackUpFile()
        {
            string sourcefile =  ConfigurationManager.AppSettings["FilePath"]; 
            string destdir = ConfigurationManager.AppSettings["BackupFilePath"];
            try
            {
                if (!Directory.Exists(destdir))
                {
                    Directory.CreateDirectory(destdir);
                }

                if (!destdir.EndsWith("/") && !destdir.EndsWith("\\"))
                    destdir += "\\";

                var destfile = $"{destdir}KMTT01FL02_Backup_{DateTime.Now.ToFileTimeUtc()}.CSV";

                File.Copy(sourcefile, destfile, true);
                Console.WriteLine("{0} was copy to {1}.", sourcefile, destdir);                
            }
            catch (Exception e)
            {
                Console.WriteLine("The process failed: {0}", e.ToString());
            }

        }
    }
}
