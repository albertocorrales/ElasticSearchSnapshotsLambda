using Amazon.Lambda.Core;
using Aws4RequestSigner;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ElasticSearchSnapshotsLambda
{
    public class Function
    {
        public async Task FunctionHandler(Request request, ILambdaContext context)
        {
            try
            {
                var accessKey = Environment.GetEnvironmentVariable("ACCESS_KEY");
                var secretKey = Environment.GetEnvironmentVariable("SECRET_KEY");
                var signer = new AWS4RequestSigner(accessKey, secretKey);
                var repositoryUrl = $"{request.Endpoint}/_snapshot/{request.RepositoryName}";
                var service = "es";

                await RegisterS3Repository(request, context, signer, repositoryUrl, service);
                await CreateSnapshot(request, context, signer, repositoryUrl, service);

            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Error performing backup: {ex.Message}");
                throw;
            }
        }

        private static async Task CreateSnapshot(Request request, ILambdaContext context, AWS4RequestSigner signer, string repositoryUrl, string service)
        {
            var snapshotName = DateTime.Now.ToString("dd-MM-yyyy-h-mm-ss").ToLower();

            context.Logger.LogLine($"Create ElasticSearch Image: {repositoryUrl}/{snapshotName}");

            var httpRequestSnapshot = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                RequestUri = new Uri($"{repositoryUrl}/{snapshotName}"),
                Content = null
            };

            httpRequestSnapshot = await signer.Sign(httpRequestSnapshot, service, request.Region);
            var client = new HttpClient();
            var responseSnapshot = await client.SendAsync(httpRequestSnapshot);

            if (!responseSnapshot.IsSuccessStatusCode)
            {
                throw new Exception($"Error creating snapshot {snapshotName}.\n" +
                    $"Error code: {responseSnapshot.StatusCode}.\n" +
                    $"Content: {await responseSnapshot.Content.ReadAsStringAsync()}");
            }
            context.Logger.LogLine($"ElasticSearch snapshot {snapshotName} successfully registered");
        }

        private static async Task RegisterS3Repository(Request request, ILambdaContext context, AWS4RequestSigner signer, string repositoryUrl, string service)
        {
            context.Logger.LogLine($"Register ElasticSearch Repository: {request.RepositoryName}");
            var requestBody = new
            {
                type = "s3",
                settings = new
                {
                    bucket = request.S3Bucket,
                    region = request.Region,
                    role_arn = request.RoleArn
                }
            };
            string requestBodyString = System.Text.Json.JsonSerializer.Serialize(requestBody);
            var content = new StringContent(requestBodyString, Encoding.UTF8, "application/json");

            var httpRequestRepository = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                RequestUri = new Uri(repositoryUrl),
                Content = content
            };

            httpRequestRepository = await signer.Sign(httpRequestRepository, service, request.Region);
            var client = new HttpClient();
            var responseRepository = await client.SendAsync(httpRequestRepository);

            if (!responseRepository.IsSuccessStatusCode)
            {
                throw new Exception($"Error registering repository {request.RepositoryName}.\n" +
                    $"Error code: {responseRepository.StatusCode}.\n" +
                    $"Content: {await responseRepository.Content.ReadAsStringAsync()}");
            }
            context.Logger.LogLine($"ElasticSearch Repository {request.RepositoryName} successfully registered");
        }
    }
}
