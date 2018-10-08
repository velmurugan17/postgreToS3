import boto3


class AwsS3():

    def __init__(self):
        self.s3 = boto3.resource('s3')


    def get_all_buckets(self):
        """

        :return:
        """
        self.buckets = []
        for bucket in self.s3.buckets.all():
            self.buckets.append(bucket.name)
        return self.buckets

    def get_all_files_in_bucket(self,bucket_name):
        """

        :param bucket_name:
        :return:
        """

        self.bucket = self.s3.Bucket(bucket_name)
        files = []
        for file in self.bucket.objects.all():
            files.append(file.key)
        return files


    def get_file_content(self,bucket_name,file_name):
        """

        :return:
        """
        file_object = self.s3.Object(bucket_name,file_name)
        return file_object.get()['Body'].read().decode('utf-8')
