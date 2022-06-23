USER ARN arn:aws:iam::030286465747:user/jared-godar
Console sign-in link https://030286465747.signin.aws.amazon.com/console

Account ID: 030286465747
User: jared-godar
SUPW

```text
python -m pip install --upgrade awscli

aws configure
AWS Access Key ID: MYACCESSKEY
AWS Secret Access Key: MYSECRETKEY
Default region name [us-west-2]: us-west-2
Default output format [None]: json

 export AWS_ACCESS_KEY_ID=<access_key>
 export AWS_SECRET_ACCESS_KEY=<secret_key>

# To use the shared credentials file, create an INI formatted file like this:

[default]
aws_access_key_id=MYACCESSKEY
aws_secret_access_key=MYSECRETKEY

[testing]
aws_access_key_id=MYACCESKEY
aws_secret_access_key=MYSECRETKEY

# and place it in ~/.aws/credentials (or in %UserProfile%\.aws/credentials on Windows)