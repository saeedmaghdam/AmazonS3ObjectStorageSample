using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace s3
{
    class Program
    {
        private static IAmazonS3 _client;

        static void Main(string[] args)
        {
            var file = "FILE_FULL_PATH";
            UploadFile(file, CancellationToken.None).GetAwaiter().GetResult();

            Console.WriteLine("Hello World!");
        }

        private async static Task UploadFile(string filePath, CancellationToken cancellationToken)
        {
            string bucketName = "BUCKET_NAME";
            string awsAccessKey = "ACCESS_KEY";
            string awsSecretKey = "SECRET_KEY";

            var config = new AmazonS3Config { ServiceURL = "S3_END_POINT" };

            _client = new AmazonS3Client(awsAccessKey, awsSecretKey, config);

            string folderPath = "test/";

            string fileKey = $"{folderPath}{System.IO.Path.GetFileName(filePath)}";
            if (await CheckExistsAsync(bucketName, fileKey, cancellationToken))
                await _client.DeleteObjectAsync(bucketName, fileKey, cancellationToken);

            await _client.PutObjectAsync(new PutObjectRequest()
            {
                InputStream = new FileInfo(filePath).OpenRead(),
                BucketName = bucketName,
                Key = fileKey
            }, cancellationToken);
        }

        private static async Task<bool> CheckExistsAsync(string bucketName, string fileKey, CancellationToken cancellationToken)
        {
            try
            {
                var res315224ponse = await _client.GetObjectMetadataAsync(new GetObjectMetadataRequest()
                {
                    BucketName = bucketName,
                    Key = fileKey
                }, cancellationToken);

                return true;
            }

            catch (Amazon.S3.AmazonS3Exception ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return false;

                throw;
            }
        }
    }
}
