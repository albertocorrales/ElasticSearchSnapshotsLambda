namespace ElasticSearchSnapshotsLambda
{
    public class Request
    {
        public string Endpoint { get; set; }
        public string RepositoryName { get; set; }
        public string S3Bucket { get; set; }
        public string Region { get; set; }
        public string RoleArn { get; set; }
    }
}
