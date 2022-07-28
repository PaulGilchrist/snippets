using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Pulte.EDH.API.Classes {
    /* Not sure why, but this is slower than ... (only 608 per second vs 1000 per second)
     * 
     *  foreach (var contact in this.DbContext.Contacts.AsNoTracking().Take(100000)) {
     *      yield return contact;
     *  }
     * 
     */
    public class TableDumper {
        public async Task DumpTableToFile(SqlConnection connection, string tableName, string destinationFile) {
            using (var command = new SqlCommand("select * from " + tableName, connection))
            using (var reader = await command.ExecuteReaderAsync())
            using (var outFile = File.CreateText(destinationFile)) {
                string[] columnNames = GetColumnNames(reader).ToArray();
                int numFields = columnNames.Length;
                await outFile.WriteLineAsync(string.Join(",", columnNames));
                if (reader.HasRows) {
                    while (await reader.ReadAsync()) {
                        string[] columnValues = 
                            Enumerable.Range(0, numFields)
                                      .Select(i => reader.GetValue(i).ToString())
                                      .Select(field => string.Concat("\"", field.Replace("\"", "\"\""), "\""))
                                      .ToArray();
                        await outFile.WriteLineAsync(string.Join(",", columnValues));
                    }
                }
            }
        }

        private IEnumerable<string> GetColumnNames(IDataReader reader) {
            foreach (DataRow row in reader.GetSchemaTable().Rows) {
                yield return (string)row["ColumnName"];
            }
        }
    }
}
