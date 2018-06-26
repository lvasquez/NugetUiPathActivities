using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace pivotSumDataTable
{
    class Program
    {
        static void Main(string[] args)
        {
            DataTable dt = new DataTable();

            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("cod", typeof(string));
            dt.Columns.Add("amount", typeof(int));

            dt.Rows.Add(1, "342", 1212);
            dt.Rows.Add(2, "654", 522);
            dt.Rows.Add(3, "3453", 1337);
            dt.Rows.Add(4, "234", 711);
            dt.Rows.Add(5, "2342", 2245);
            dt.Rows.Add(1, "233", 1000);

            //DataTable mydt = Pivot2(dt, dt.Columns["amount"], dt.Columns["amount"]);
            DataTable mydt = piv(dt, "Id", "amount");

            foreach (DataRow i in mydt.Rows)
            {
                Console.WriteLine(i.ItemArray[0] + " - " + i.ItemArray[2]);
            }
            Console.ReadKey();
        }


        static DataTable piv(DataTable dt, string idColumn, string valueColumn)
        {

            var newDt = dt.AsEnumerable()
              .GroupBy(r => r.Field<int>(idColumn))
              .Select(g =>
              {
                  var row = dt.NewRow();

                  row[idColumn] = g.Key;
                  row[valueColumn] = g.Sum(r => r.Field<int>(valueColumn));

                  return row;
              }).CopyToDataTable();

            return newDt;
        }

        static DataTable Pivot2(DataTable dt, DataColumn pivotColumn, DataColumn pivotValue)
        {
            // find primary key columns 
            //(i.e. everything but pivot column and pivot value)
            DataTable temp = dt.Copy();
            temp.Columns.Remove(pivotColumn.ColumnName);
            temp.Columns.Remove(pivotValue.ColumnName);
            string[] pkColumnNames = temp.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .ToArray();

            // prep results table
            DataTable result = temp.DefaultView.ToTable(true, pkColumnNames).Copy();
            result.PrimaryKey = result.Columns.Cast<DataColumn>().ToArray();
            dt.AsEnumerable()
                .Select(r => r[pivotColumn.ColumnName].ToString())
                .Distinct().ToList()
                .ForEach(c => result.Columns.Add(c, pivotColumn.DataType));

            // load it
            foreach (DataRow row in dt.Rows)
            {
                // find row to update
                DataRow aggRow = result.Rows.Find(
                    pkColumnNames
                        .Select(c => row[c])
                        .ToArray());
                // the aggregate used here is LATEST 
                // adjust the next line if you want (SUM, MAX, etc...)
                aggRow[row[pivotColumn.ColumnName].ToString()] = row[pivotValue.ColumnName];
            }

            return result;
        }


        static DataTable Pivot(DataTable dt, DataColumn pivotColumn, DataColumn pivotValue)
        {
            DataColumn sumColumn = new DataColumn();
            sumColumn.ColumnName = "SUM";
            sumColumn.DataType = typeof(int);
            sumColumn.DefaultValue = 0;

            // find primary key columns 
            //(i.e. everything but pivot column and pivot value)
            DataTable temp = dt.Copy();
            temp.Columns.Remove(pivotColumn.ColumnName);
            temp.Columns.Remove(pivotValue.ColumnName);
            string[] pkColumnNames = temp.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .ToArray();

            // prep results table
            DataTable result = temp.DefaultView.ToTable(true, pkColumnNames).Copy();
            result.PrimaryKey = result.Columns.Cast<DataColumn>().ToArray();
            // include the sum column
            result.Columns.Add(sumColumn);
            dt.AsEnumerable()
                .Select(r => r[pivotColumn.ColumnName].ToString())
                .Distinct()
                .OrderBy(c => Convert.ToDateTime(c))
                .ToList()
                .ForEach(c => result.Columns.Add(c, pivotColumn.DataType));

            // load it
            foreach (DataRow row in dt.Rows)
            {
                // find row to update
                DataRow aggRow = result.Rows.Find(
                    pkColumnNames
                        .Select(c => row[c])
                        .ToArray());
                // the aggregate used here is LATEST 
                // adjust the next line if you want (SUM, MAX, etc...)
                string columnName = row[pivotColumn.ColumnName].ToString();
                if (aggRow.IsNull(columnName))
                {
                    aggRow[columnName] = (int)row[pivotValue.ColumnName];
                }
                else
                {
                    aggRow[columnName] = Convert.ToInt32(aggRow[columnName]) +
                                         (int)row[pivotValue.ColumnName];
                }
                // add the value to the sum
                aggRow[sumColumn] = (int)aggRow[sumColumn] +
                                    (int)row[pivotValue.ColumnName];
            }

            return result;
        }
    }
}
