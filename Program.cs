using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.Threading;
using System.IO;
using System.Configuration;

namespace scrollproject
{
    class Program
    {
        static void Main()
        {
            //Thread.CurrentThread.CurrentCulture = new CultureInfo("ja-JP");

            try
            {
                var filelocation = ConfigurationManager.AppSettings["FilePath"];
                Log.Trace("プログラムの開始...");
                DataAcess da = new DataAcess();
                da.TruncateFileRecordInsertedTable();
                da.UploadFile(filelocation);
                da.DeleteExistingData();
                da.UploadDataToFinalTable();
                da.BackUpFile();
                Log.Trace("プログラムを完了しました...");
            }
            catch (Exception ex)
            {
                Log.Error($"{ex}");
            }
            finally
            {
                Console.WriteLine("任意のキーを押して続行します。");
                Log.printline();
                Console.ReadKey();

            }
        }
    }
}
