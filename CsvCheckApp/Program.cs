using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.IO;
using CsvHelper;
using Npgsql;

namespace CsvCheckApp
{
  internal static class Program
  {
    static void Main()
    {
      // This is a simple console application that checks if the CSV file is valid.
      // It reads the CSV file and checks if it has the correct number of columns.
      // If the CSV file is valid, it prints "CSV file is valid." to the console.
      // If the CSV file is invalid, it prints "CSV file is invalid." to the console.
      Console.WriteLine("CSV Check Application");
      Console.WriteLine("Checking CSV file...");
      const string connectionString = "Host=localhost;Username=postgres;Password=motdepasse;Database=maBase";
      const string csvPath = @"C:\temp\monfichier.csv";
      const string tableName = "tableCible";

      // Détecter séparateur du fichier CSV
      char separator = DetectCsvSeparator(csvPath);
      Console.WriteLine($"Séparateur détecté : '{separator}'");
      if (separator == '\0')
      {
        Console.WriteLine("Erreur : Impossible de détecter le séparateur du fichier CSV.");
        return;
      }
      
      Console.WriteLine($"Séparateur utilisé : '{separator}'");

      using (var conn = new NpgsqlConnection(connectionString))
      {
        if (!OpenConnection(connectionString))
        {
          Console.WriteLine("Échec d'ouverture de la connexion à la base de données PostgreSQL.");
          return;
        }

        Console.WriteLine("Connexion à la base de données réussie.");

        // Récupérer les tailles max des colonnes
        var columnSizes = GetColumnSizes(conn, tableName);

        var errors = ValidateCsv(conn, csvPath, tableName, columnSizes);
        Console.WriteLine("=== Erreurs détectées ===");
        foreach (var err in errors)
        {
          Console.WriteLine(err);
        }

        ProcessCsv(conn, csvPath, tableName, columnSizes);
      }

      Console.WriteLine("Press any key to exit:");
      Console.ReadKey();
    }

    /// <summary>
    /// Process the CSV file and insert valid rows into the PostgreSQL table.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="csvPath">The path to the CSV file.</param>
    /// <param name="tableName">The name of the table to insert into.</param>
    /// <param name="columnSizes">A dictionary mapping column names to their maximum sizes.</param>
    static void ProcessCsv(NpgsqlConnection connection, string csvPath, string tableName, Dictionary<string, int?> columnSizes)
    {
      var validRows = GetValidRows(connection, csvPath, tableName, columnSizes);

      if (validRows.Count == 0)
      {
        Console.WriteLine("Aucune ligne valide à insérer.");
        return;
      }

      List<string> columnNames = new List<string>(columnSizes.Keys);

      if (validRows.Count > 100)
      {
        Console.WriteLine("Utilisation de BulkInsert (plus de 100 lignes)...");
        BulkInsertValidRows(connection, tableName, validRows, columnNames);
      }
      else
      {
        Console.WriteLine("Utilisation de Insert ligne par ligne (100 ou moins)...");
        InsertValidRows(connection, tableName, validRows, columnNames);
      }

      Console.WriteLine($"{validRows.Count} lignes insérées avec succès.");
    }

    /// <summary>
    /// Insert valid rows into the PostgreSQL table using bulk insert.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="tableName">The name of the table to insert into.</param>
    /// <param name="validRows">A list of valid rows to insert.</param>
    /// <param name="columnNames">A list of column names corresponding to the values in each row.</param>
    static void BulkInsertValidRows(NpgsqlConnection connection, string tableName, List<List<string>> validRows, List<string> columnNames)
    {
      using (var writer = connection.BeginBinaryImport($"COPY {tableName} ({string.Join(",", columnNames)}) FROM STDIN (FORMAT BINARY)"))
      {
        foreach (var row in validRows)
        {
          writer.StartRow();
          foreach (var val in row)
          {
            writer.Write(val);
          }
        }

        writer.Complete();
      }
    }

    /// <summary>
    /// Detect the CSV separator character by analyzing the header line.
    /// </summary>
    /// <param name="filePath">The path to the CSV file.</param>
    /// <returns>The detected CSV separator character.</returns>
    static char DetectCsvSeparator(string filePath)
    {
      using (var reader = new StreamReader(filePath))
      {
        string headerLine = reader.ReadLine() ?? "";
        int commaCount = headerLine.Split(',').Length;
        int semicolonCount = headerLine.Split(';').Length;

        return (semicolonCount > commaCount) ? ';' : ',';
      }
    }

    /// <summary>
    /// Open a connection to the PostgreSQL database.
    /// </summary>
    /// <param name="connectionString">The connection string for the PostgreSQL database.</param>
    /// <returns>A boolean indicating whether the connection was successful.</returns>
    static bool OpenConnection(string connectionString)
    {
      try
      {
        var conn = new NpgsqlConnection(connectionString);
        conn.Open();
        return true;
      }
      catch (Exception)
      {
        return false;
      }
    }

    /// <summary>
    /// Get the maximum sizes of columns in a PostgreSQL table.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="tableName">The name of the table to inspect.</param>
    /// <returns>A dictionary mapping column names to their maximum sizes.</returns>
    static Dictionary<string, int?> GetColumnSizes(NpgsqlConnection connection, string tableName)
    {
      var sizes = new Dictionary<string, int?>();

      const string sql = @"
                SELECT column_name, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = @table
                ORDER BY ordinal_position;";

      using (var cmd = new NpgsqlCommand(sql, connection))
      {
        cmd.Parameters.AddWithValue("table", tableName);
        using (var reader = cmd.ExecuteReader())
        {
          while (reader.Read())
          {
            string colName = reader.GetString(0);
            int? maxLength = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
            sizes[colName] = maxLength;
          }
        }
      }

      return sizes;
    }

    /// <summary>
    /// Validate the CSV file against the PostgreSQL table.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="csvPath">The path to the CSV file.</param>
    /// <param name="tableName">The name of the table to validate against.</param>
    /// <param name="columnSizes">A dictionary mapping column names to their maximum sizes.</param>
    /// <returns>A list of error messages, if any.</returns>
    static List<string> ValidateCsv(NpgsqlConnection connection, string csvPath, string tableName, Dictionary<string, int?> columnSizes)
    {
      var errors = new List<string>();

      using (var reader = new StreamReader(csvPath))
      using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
      {
        var records = csv.GetRecords<dynamic>();

        int lineNumber = 1; // Pour indiquer l'emplacement de l'erreur
        foreach (ExpandoObject record in records)
        {
          var dict = (IDictionary<string, object>)record;

          // Vérifier les longueurs
          foreach (var col in dict)
          {
            if (columnSizes.ContainsKey(col.Key) && columnSizes[col.Key].HasValue)
            {
              string value = col.Value?.ToString() ?? "";
              if (value.Length > columnSizes[col.Key].Value)
              {
                errors.Add($"Ligne {lineNumber} : Champ '{col.Key}' trop long ({value.Length}/{columnSizes[col.Key].Value}) → valeur='{value}'");
              }
            }
          }

          // Vérifier doublons
          string whereClause = "";
          var parameters = new List<NpgsqlParameter>();
          int i = 0;

          foreach (var col in dict)
          {
            if (i > 0)
            {
              whereClause += " AND ";
            }

            whereClause += $"{col.Key} = @p{i}";
            parameters.Add(new NpgsqlParameter($"p{i}", col.Value ?? DBNull.Value));
            i++;
          }

          string sqlCheck = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
          using (var cmd = new NpgsqlCommand(sqlCheck, connection))
          {
            cmd.Parameters.AddRange(parameters.ToArray());
            long count = (long)cmd.ExecuteScalar();

            if (count > 0)
            {
              errors.Add($"Ligne {lineNumber} : Doublon trouvé en base → {string.Join(", ", dict.Values)}");
            }
          }

          lineNumber++;
        }
      }

      return errors;
    }

    /// <summary>
    /// Get valid rows from the CSV file based on column sizes and existing data in the PostgreSQL table.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="csvPath">The path to the CSV file.</param>
    /// <param name="tableName">The name of the table to validate against.</param>
    /// <param name="columnSizes">A dictionary mapping column names to their maximum sizes.</param>
    /// <returns>A list of valid rows from the CSV file.</returns>
    static List<List<string>> GetValidRows(NpgsqlConnection connection, string csvPath, string tableName, Dictionary<string, int?> columnSizes)
    {
      var validRows = new List<List<string>>();

      using (var reader = new StreamReader(csvPath))
      using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
      {
        var records = csv.GetRecords<dynamic>();

        foreach (ExpandoObject record in records)
        {
          var dict = (IDictionary<string, object>)record;
          bool isValid = true;

          // Vérifier longueurs
          foreach (var column in dict)
          {
            if (columnSizes.ContainsKey(column.Key) && columnSizes[column.Key].HasValue)
            {
              string value = column.Value?.ToString() ?? "";
              if (value.Length > columnSizes[column.Key].Value)
              {
                isValid = false;
                break;
              }
            }
          }

          // Vérifier doublons
          if (isValid)
          {
            string whereClause = "";
            var parameters = new List<NpgsqlParameter>();
            int i = 0;

            foreach (var col in dict)
            {
              if (i > 0)
              {
                whereClause += " AND ";
              }

              whereClause += $"{col.Key} = @p{i}";
              parameters.Add(new NpgsqlParameter($"p{i}", col.Value ?? DBNull.Value));
              i++;
            }

            string sqlCheck = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
            using (var cmd = new NpgsqlCommand(sqlCheck, connection))
            {
              cmd.Parameters.AddRange(parameters.ToArray());
              long count = (long)cmd.ExecuteScalar();

              if (count > 0)
              {
                isValid = false;
              }
            }
          }

          if (isValid)
          {
            var rowValues = new List<string>();
            foreach (var val in dict.Values)
            {
              rowValues.Add(val?.ToString() ?? "");
            }

            validRows.Add(rowValues);
          }
        }
      }

      return validRows;
    }

    /// <summary>
    /// Insert valid rows into the PostgreSQL table.
    /// </summary>
    /// <param name="connection">The PostgreSQL connection.</param>
    /// <param name="tableName">The name of the table to insert into.</param>
    /// <param name="validRows">A list of valid rows to insert.</param>
    /// <param name="columnNames">A list of column names corresponding to the values in each row.</param>
    static void InsertValidRows(NpgsqlConnection connection, string tableName, List<List<string>> validRows, List<string> columnNames)
    {
      if (validRows.Count == 0)
      {
        return;
      }

      var sql = $"INSERT INTO {tableName} ({string.Join(",", columnNames)}) VALUES ";
      var values = new List<string>();
      var parameters = new List<NpgsqlParameter>();
      int paramIndex = 0;

      foreach (var row in validRows)
      {
        var paramNames = new List<string>();
        for (int i = 0; i < row.Count; i++)
        {
          string paramName = $"@p{paramIndex}";
          paramNames.Add(paramName);
          parameters.Add(new NpgsqlParameter(paramName, row[i] ?? (object)DBNull.Value));
          paramIndex++;
        }

        values.Add($"({string.Join(",", paramNames)})");
      }

      sql += string.Join(",", values);

      using (var cmd = new NpgsqlCommand(sql, connection))
      {
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.ExecuteNonQuery();
      }
    }
  }
}
