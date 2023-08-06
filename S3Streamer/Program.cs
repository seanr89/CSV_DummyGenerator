using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.Runtime;
using Amazon.S3.Model;

// See https://aka.ms/new-console-template for more information
//Console.WriteLine("Hello, World!");


    //1million
    int totalCount = 10000000;
    int increment = 1000000;
    int page = 0;
    bool isFirstStream = true;
    bool isLastStream = false;
    // Create list to store upload part responses.
    List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();


await runApp();

async Task runApp(){
    var resp = new UploadResp();
    //string resp = "";
    Console.WriteLine("Hello, World!");


    do{

        //using(MemoryStream stream = new MemoryStream())
        //{
            var stream = new MemoryStream();
        
            using (var writer = new StreamWriter(stream))
            {
                foreach (var item in Enumerable.Range(0, increment))
                {
                    writer.WriteLine($"Writing content here {item}");
                }
                writer.Flush();

                //Console.WriteLine($"Page: {page} and count = {page * increment} and streamSize = {stream.Length}");
                if((page * increment) >= totalCount)
                    isLastStream = true;
                //TODO push data to S3
                resp = await UploadPartStream(stream, resp.UploadId, (page + 1), isFirstStream, isLastStream, resp.Etags);
                isFirstStream = false;
                page++;
            }
        //}
    }while((page * increment) <= totalCount);

}



/// <summary>
/// 
/// </summary>
/// <param name="stream"></param>
/// <param name="uploadId"></param>
/// <param name="prevETags"></param>
/// <returns></returns>
async Task<UploadResp> UploadPartStream(MemoryStream stream, string uploadId, int partIndex, bool isFirstStream, bool isLastStream, string prevETags){
    var res = new UploadResp();
    string response = "";
    Console.WriteLine($"UploadPartStream: UploadId = {uploadId} and partIndex = {partIndex}");

        RegionEndpoint BucketRegion = RegionEndpoint.EUWest1;
        var _s3Client = new AmazonS3Client(BucketRegion);
    try{
        // Retreiving Previous ETags
        var eTags = new List<PartETag>();
        if (!string.IsNullOrEmpty(prevETags))
        {
            eTags = SetAllETags(prevETags);
        }


        // var ms = new MemoryStream();
        // stream.CopyTo(ms);
        // ms.Position = 0;
        long partSize = 5 * (long)Math.Pow(2, 20); // 5 MB

        Console.WriteLine("Upload Part Size: {0}", stream.Length);

        if (isFirstStream)
        {
            Console.WriteLine("Upload First Stream");
            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = "sean-csv-bucketwriter",
                Key = "multi-file-test.txt"
            };

            var initResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);
            uploadId = initResponse.UploadId;
            Console.WriteLine("Upload First Stream: {0} and length {1}", initResponse.UploadId, );
        }

        var ms = new MemoryStream();
        stream.CopyTo(ms);
        ms.Position = 0;

        //Step 2: upload each chunk (this is run for every chunk unlike the other steps which are run once)
        var uploadRequest = new UploadPartRequest
        {
            BucketName = "sean-csv-bucketwriter",
            Key = "multi-file-test.txt",
            UploadId = uploadId,
            PartNumber = partIndex,
            InputStream = ms,
            IsLastPart = isLastStream,
            PartSize = ms.Length
        };

        // TODO - comment
        var uploadResponse = await _s3Client.UploadPartAsync(uploadRequest);
        if(isFirstStream == false)
        {
            uploadResponses.Add(uploadResponse);
        }
        //Console.WriteLine($"UploadPartStream ContentLength: {uploadResponse.ContentLength} and etag = {uploadResponse.ETag}");
        if (isLastStream)
        {
            ListMultipartUploadsRequest listRequest = new ListMultipartUploadsRequest
            {
                BucketName = "sean-csv-bucketwriter",
                KeyMarker = uploadId
            };

            var listResponse = await _s3Client.ListMultipartUploadsAsync(listRequest);
            Console.WriteLine("Upload Last Stream List Part: {0} and count {1}", listResponse.HttpStatusCode, listResponse.MultipartUploads.Count);


            //Console.WriteLine("Upload Last Stream: {0}", partIndex);
            eTags.Add(new PartETag
            {
                PartNumber = partIndex,
                ETag = uploadResponse.ETag
            });

            //Console.WriteLine("Upload Last Stream Tags: {0}", eTags);
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = "sean-csv-bucketwriter",
                Key = "multi-file-test.txt",
                UploadId = uploadId,
            };
            completeRequest.AddPartETags(uploadResponses);

            //Console.WriteLine("Upload Last Stream: {0}", completeRequest.ToString());

            var result = await _s3Client.CompleteMultipartUploadAsync(completeRequest);
            res.UploadId = uploadRequest.UploadId;
            //Set the uploadId and fileURLs with the response.
            response = uploadRequest.UploadId + "|success|" + result.Location + "|";
            //For image get thumbnail url
            response += "";
            res.Etags = response;
        }
        else
        {
            //Console.WriteLine("Upload Add ETags");
            eTags.Add(new PartETag
            {
                PartNumber =  partIndex,
                ETag = uploadResponse.ETag
            });

            res.UploadId = uploadRequest.UploadId;

            //Set the uploadId and eTags with the response
            // response = uploadRequest.UploadId + "|" + GetAllETags(eTags);
            response = GetAllETags(eTags);
            res.Etags = response;
        }
    }
    catch (Exception e)
    {
        Console.WriteLine($"Exception: {e.Message}");
        //Console.WriteLine($"Inner Exception: {e.InnerException.Message}");
        Console.WriteLine($"Stack Trace: {e.StackTrace}");
        await _s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest()
        {
            BucketName = "sean-csv-bucketwriter",
            Key = "multi-file-test.txt",
            UploadId = uploadId
        });
        return null;
    }
    //Console.WriteLine($"UploadPartStream: Done");
    return res;
}


// void AbortPartToCloud(string uploadId)
// {
//     // abort.
//     client.AbortMultipartUpload(new AbortMultipartUploadRequest()
//     {
//         BucketName = "sean-csv-bucketwriter",
//         Key = "multi-file-test.txt",
//         UploadId = uploadId
//     });
// }

List<PartETag> SetAllETags(string prevETags)
{
    //Console.WriteLine($"SetAllETags: {prevETags}");
    var partETags = new List<PartETag>();
    var splittedPrevETags = prevETags.Split(',');

    for (int i = 0; i < splittedPrevETags.Length; i++)
    {
        partETags.Add(new PartETag
        {
            PartNumber = Int32.Parse(splittedPrevETags[i]),
            //PartNumber = i + 1,
            ETag = splittedPrevETags[i + 1]
        });

        i = i + 1;
    }

    return partETags;
}

string GetAllETags(List<PartETag> newETags)
{
    var newPartETags = "";
    var isNotFirstTag = false;

    foreach (var eTag in newETags)
    {
        newPartETags += ((isNotFirstTag) ? "," : "") + (eTag.PartNumber.ToString() + ',' + eTag.ETag);
        isNotFirstTag = true;
    }
    //Console.WriteLine($"GetAllETags: {newPartETags}");
    return newPartETags;
}

// static void UploadPartProgressEventCallback(object sender, StreamTransferProgressArgs e)
// {
//     // Process event. 
//     Console.WriteLine("{0}/{1}", e.TransferredBytes, e.TotalBytes);
// }