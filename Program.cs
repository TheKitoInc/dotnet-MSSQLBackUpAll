using System;
using System.Data;
using System.IO;
using System.IO.Compression;
using Microsoft.Data.SqlClient;

class Program
{
    static void Main()
    {
        //System.Net.ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;

        string connectionString = "Server=localhost;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;";
        string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");

        try
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();

            string backupDir = GetDefaultBackupDirectory(connection);
            Console.WriteLine($"📁 Default backup directory: {backupDir}");

            var gzipTasks = new List<Task>();

            foreach (string dbName in GetOnlineDatabases(connection))
            {
                string backupFile = Path.Combine(backupDir, $"{dbName}_{timestamp}.bak");

                Console.WriteLine($"\n📦 Backing up: {dbName}");

                BackupDatabase(connection, dbName, backupFile);

                gzipTasks.Add(Task.Run(() => Gzip(backupFile)));
            }

            Console.WriteLine("✔️ All backups completed. Waiting for compression...");

            Task.WaitAll(gzipTasks.ToArray());  // Wait for all tasks to complete before exiting
            Console.WriteLine("✔️ All compression completed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("❌ Error:");
            Console.WriteLine(ex.Message);
        }

    }

    static string GetDefaultBackupDirectory(SqlConnection connection)
    {
        using var cmd = new SqlCommand(@"
            DECLARE @path NVARCHAR(512)
            EXEC master.dbo.xp_instance_regread 
                N'HKEY_LOCAL_MACHINE', 
                N'Software\Microsoft\MSSQLServer\MSSQLServer', 
                N'BackupDirectory', 
                @path OUTPUT, 
                'no_output'
            SELECT @path AS BackupDirectory;", connection);

        var result = cmd.ExecuteScalar();
        return result?.ToString() ?? throw new Exception("Unable to retrieve backup directory.");
    }

    static string[] GetOnlineDatabases(SqlConnection connection)
    {
        using var cmd = new SqlCommand(@"
            SELECT name 
            FROM sys.databases 
            WHERE name NOT IN ('tempdb') 
              AND state_desc = 'ONLINE'", connection);

        using var reader = cmd.ExecuteReader();
        var dbs = new System.Collections.Generic.List<string>();
        while (reader.Read())
        {
            dbs.Add(reader.GetString(0));
        }
        return dbs.ToArray();
    }

    static void BackupDatabase(SqlConnection connection, string dbName, string filePath)
    {
        string sql = $@"
            BACKUP DATABASE [{dbName}]
            TO DISK = N'{filePath}'
            WITH INIT, FORMAT, NAME = N'{dbName} - Full Backup';";

        using var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = 3600; // 1 hour timeout
        cmd.ExecuteNonQuery();

        Console.WriteLine($"✔️ Backup saved: {filePath}");
    }

    static void Gzip(string sourceFile)
    {
        if (!File.Exists(sourceFile))
        {
            Console.WriteLine($"❌ Source file does not exist: {sourceFile}");
            return;
        }

        string destinationFile = sourceFile + ".gz";

        try
        {
            if (File.Exists(destinationFile))
            {
                File.Delete(destinationFile);
            }

            using FileStream originalFileStream = File.OpenRead(sourceFile);
            using FileStream compressedFileStream = File.Create(destinationFile);
            using GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress);

            originalFileStream.CopyTo(compressionStream);
            Console.WriteLine($"✔️ Compressed: {sourceFile} → {destinationFile}");
        }
        catch (Exception ex)
        {
            Console.WriteLine("❌ Compression failed:");
            Console.WriteLine(ex.Message);
            return; // Important to return here to avoid deleting the original file if compression fails
        }

        try
        {
            File.Delete(sourceFile);
            Console.WriteLine($"✔️ Deleted original: {sourceFile}");
        }
        catch (Exception ex)
        {
            Console.WriteLine("❌ Failed to delete original file:");
            Console.WriteLine(ex.Message);
        }
    }
}
