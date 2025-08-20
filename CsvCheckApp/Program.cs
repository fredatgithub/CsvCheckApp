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

      using (var conn = new NpgsqlConnection(connectionString))
      {
        conn.Open();

        // Récupérer les tailles max des colonnes
        var columnSizes = GetColumnSizes(conn, tableName);

        // Vérifier et obtenir les erreurs
        var errors = ValidateCsv(conn, csvPath, tableName, columnSizes);

        Console.WriteLine("=== Erreurs détectées ===");
        foreach (var err in errors)
          Console.WriteLine(err);

        // Obtenir les lignes valides
        var validRows = GetValidRows(conn, csvPath, tableName, columnSizes);

        Console.WriteLine("=== Lignes valides (non doublons, tailles correctes) ===");
        foreach (var row in validRows)
        {
          Console.WriteLine(string.Join(" | ", row));
        }
      }

      Console.WriteLine("Vérification terminée.");

      Console.WriteLine("Press any key to exit:");
      Console.ReadKey();
    }

    static NpgsqlConnection CreateConnection(string connectionString)
    {
      var conn = new NpgsqlConnection(connectionString);
      conn.Open();
      return conn;
    }

    static Dictionary<string, int?> GetColumnSizes(NpgsqlConnection conn, string tableName)
    {
      var sizes = new Dictionary<string, int?>();

      string sql = @"
                SELECT column_name, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = @table
                ORDER BY ordinal_position;";

      using (var cmd = new NpgsqlCommand(sql, conn))
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

    static List<string> ValidateCsv(NpgsqlConnection conn, string csvPath, string tableName, Dictionary<string, int?> columnSizes)
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
            if (i > 0) whereClause += " AND ";
            whereClause += $"{col.Key} = @p{i}";
            parameters.Add(new NpgsqlParameter($"p{i}", col.Value ?? DBNull.Value));
            i++;
          }

          string sqlCheck = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
          using (var cmd = new NpgsqlCommand(sqlCheck, conn))
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

    static List<List<string>> GetValidRows(NpgsqlConnection conn, string csvPath, string tableName, Dictionary<string, int?> columnSizes)
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
          foreach (var col in dict)
          {
            if (columnSizes.ContainsKey(col.Key) && columnSizes[col.Key].HasValue)
            {
              string value = col.Value?.ToString() ?? "";
              if (value.Length > columnSizes[col.Key].Value)
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
              if (i > 0) whereClause += " AND ";
              whereClause += $"{col.Key} = @p{i}";
              parameters.Add(new NpgsqlParameter($"p{i}", col.Value ?? DBNull.Value));
              i++;
            }

            string sqlCheck = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
            using (var cmd = new NpgsqlCommand(sqlCheck, conn))
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
              rowValues.Add(val?.ToString() ?? "");
            validRows.Add(rowValues);
          }
        }
      }

      return validRows;
    }
  }
}
