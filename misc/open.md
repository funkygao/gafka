# Open Platform

### facebook 

A developer can create N apps(App Id, App Secret)
e,g.  App Id=588231398913851  App Secret=ad09ce23278038fdebbc806ca2dce90d

    type App struct {
        Id, Secret string
        Domains []string
        Category ref
        Icon blob
        DisplayName, Namespace string
        ContactEmail string
        PrivacyPolicyURL, TermOfServiceURL string

        // entry point
        SecureCanvasURL string
        MobileSiteURL   string
    }

#### Testing

create a separate Facebook user account:

#### Access Token
  - user access token (login dialog)
  - app access token (server-to-server)

  - short-term token 1-2h
  - long-term token 60d

### aws IAM

There is great distinguation between account and user/group.

#### Terms
When you sign up for AWS, you provide an email address and password that is associated with your AWS account.
The account email address and password are root-level credentials, and anyone who uses these credentials has full access to all resources in the account. 
We recommend that you can use an IAM user name and password to sign in to AWS web pages.

When multiple individuals or applications require access to your AWS account, IAM lets you create unique IAM user identities. 
Users can use their own user names and passwords to sign in to the AWS Management Console, AWS discussion forums, or AWS support center.

Access keys consist of an access key ID (for example, AKIAIOSFODNN7EXAMPLE) and a secret access key (for example, wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY). 
You use access keys to sign programmatic requests that you make to AWS.

You can also create and use temporary access keys, known as temporary security credentials. 
In addition to the access key ID and secret access key, temporary security credentials include a security token that you must send to AWS when you use temporary security credentials. 

#### Credentials

IAM is a web service that you can use to manage users and user permissions under your AWS account.

    id:     AWS_ACCESS_KEY_ID
    secret: AWS_SECRET_ACCESS_KEY

    AWS_SESSION_TOKEN

    type Provider interface {
        Retrieve() (Value, error)
        IsExpired() bool
    }

    type Value struct {
        AccessKeyID, SecretAccessKey string
        SessionToken string
    }
   
   signed request header:
   Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7
   Authorization: AWS3 AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE,Algorithm=HmacSHA256,SignedHeaders=Host;X-Amz-Date;X-Amz-Target;Content-Encoding,Signature=dv0H1RPYucoIcRckspWO0f8xG120MWZRKmj3O5/A4rY=

#### Federation/Delegation

With identity federation, external identities (federated users) are granted secure access to resources in your AWS account without having to create IAM users.

SAML Security Assertion Markup Language

#### Policy

    type Policy struct {
        Version string
        Statements []Statement
    }

    type Statement struct {
        Effect string // Allow/Deny
        Action string // the specific API action e,g. ec2:Describe
        Resource string // Amazon Resource Name(arn)
        Condition map[string]map[string]string // condition: key->value e,g. StringEquals : ec2:ResourceTag/Environment : Development
    }

#### Kinesis policy example

Action
- kinesis:CreateStream
- kinesis:ListStreams
- kinesis:DescribeStream

Resource: arn:aws:kinesis:{region}:{account-id}:stream/{stream-name}

A full example

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "kinesis: Get*"
                ],
                "Resource": [
                    "arn:aws:kinesis:us-east-1:111122223333:stream/stream1"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream"
                ],
                "Resource": [
                    "arn:aws:kinesis:us-east-1:111122223333:stream/stream1"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "kinesis:ListStreams"
                ],
                "Resource": [
                    "*"
                ]
            }
            {
               "Effect": "Allow",
               "Action": [
                   "kinesis:PutRecord"
               ],
               "Resource": [
                   "arn:aws:kinesis:us-east-1:111122223333:stream/*"
               ]
            }
        ]
    }
    
