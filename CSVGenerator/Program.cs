using Newtonsoft.Json;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

int count = 0;
int increment = 10000;
int total = 100000;

var dataArray = new List<DataModel>();

for (int i = 0; i < total; i++)
{
    var data = new DataModel
    {
        customerId = Guid.NewGuid(),
        offerId = Guid.NewGuid(),
        startDate = DateTime.Now,
        endDate = DateTime.Now.AddDays(30),
        activation = null
    };

    dataArray.Add(data);

    count++;

    if (count == increment)
    {
        Console.WriteLine($"Added {count} records");
        count = 0;
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            // Don't write the header again.
            HasHeaderRecord = false,
        };
        using (var stream = File.Open("data.csv", FileMode.Append))
        using (var writer = new StreamWriter(stream))
        using (var csv = new CsvWriter(writer, config))
        {
            csv.WriteRecords(dataArray);
        }
        // using(StreamWriter file = new StreamWriter("data.csv", true))
        // {
        //     file.WriteLine(JsonConvert.SerializeObject(dataArray));
        // }
        if(dataArray.Count > 100000)
        {
            dataArray.Clear();
        }
    }
}

dataArray.Clear();

//Console.WriteLine($"Added {dataArray.Count} records");

string fileName = "data.csv";
FileInfo fi = new FileInfo(fileName);

long size = fi.Length;
Console.WriteLine("File Size in Bytes: {0}", size);
var sizeInMb = size / 1000000;
Console.WriteLine("File Size in MB: {0}", sizeInMb);

// re-read the file and output the number of lines
var lineCount = File.ReadLines(fileName).Count();
Console.WriteLine("Number of lines: {0}", lineCount.ToString("N0"));