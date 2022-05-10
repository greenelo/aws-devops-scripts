import logging
import boto3

AWS_ACCOUNT_ID = ""
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

REGION = "ap-east-1"

CONFIGSERVICE_RESOURCE_TYPES = ["AWS::EC2::CustomerGateway","AWS::EC2::EIP","AWS::EC2::Host","AWS::EC2::Instance","AWS::EC2::InternetGateway","AWS::EC2::NetworkAcl","AWS::EC2::NetworkInterface","AWS::EC2::RouteTable","AWS::EC2::SecurityGroup","AWS::EC2::Subnet","AWS::CloudTrail::Trail","AWS::EC2::Volume","AWS::EC2::VPC","AWS::EC2::VPNConnection","AWS::EC2::VPNGateway","AWS::IAM::Group","AWS::IAM::Policy","AWS::IAM::Role","AWS::IAM::User","AWS::ACM::Certificate","AWS::RDS::DBInstance","AWS::RDS::DBSubnetGroup","AWS::RDS::DBSecurityGroup","AWS::RDS::DBSnapshot","AWS::RDS::EventSubscription","AWS::ElasticLoadBalancingV2::LoadBalancer","AWS::S3::Bucket","AWS::SSM::ManagedInstanceInventory","AWS::Redshift::Cluster","AWS::Redshift::ClusterSnapshot","AWS::Redshift::ClusterParameterGroup","AWS::Redshift::ClusterSecurityGroup","AWS::Redshift::ClusterSubnetGroup","AWS::Redshift::EventSubscription","AWS::CloudWatch::Alarm","AWS::CloudFormation::Stack","AWS::DynamoDB::Table","AWS::AutoScaling::AutoScalingGroup","AWS::AutoScaling::LaunchConfiguration","AWS::AutoScaling::ScalingPolicy","AWS::AutoScaling::ScheduledAction","AWS::CodeBuild::Project","AWS::WAF::RateBasedRule","AWS::WAF::Rule","AWS::WAF::WebACL","AWS::WAFRegional::RateBasedRule","AWS::WAFRegional::Rule","AWS::WAFRegional::WebACL","AWS::CloudFront::Distribution","AWS::CloudFront::StreamingDistribution","AWS::WAF::RuleGroup","AWS::WAFRegional::RuleGroup","AWS::Lambda::Function","AWS::ElasticBeanstalk::Application","AWS::ElasticBeanstalk::ApplicationVersion","AWS::ElasticBeanstalk::Environment","AWS::ElasticLoadBalancing::LoadBalancer","AWS::XRay::EncryptionConfig","AWS::SSM::AssociationCompliance","AWS::SSM::PatchCompliance","AWS::Shield::Protection","AWS::ShieldRegional::Protection","AWS::Config::ResourceCompliance","AWS::CodePipeline::Pipeline"]

class Resource:
    def __init__(self, id, type):
        self.id = id
        self.type = type

def log(level: int, message: str):
    logging.log(level, message)
    print(message)

def create_resource_if_not_exists_in(id, type, resource_list):
    if not filter(lambda x: x.id == id, resource_list):
        return Resource(id, type)

def list_resources_by_configservice(resource_list):
    client = boto3.client('config')
    for resource_type in CONFIGSERVICE_RESOURCE_TYPES:
        next_token = "something"
        while next_token:
            if next_token == "something":
                response = client.list_discovered_resources(resourceType=resource_type)
            else:
                response = client.list_discovered_resources(resourceType=resource_type, nextToken=next_token)
            if 'nextToken' in response:
                next_token = response['nextToken']
            else:
                next_token = None
            if response['resourceIdentifiers']:
                resource_list += [x for x in map(lambda x: create_resource_if_not_exists_in(x['resourceId'], x['resourceType'], resource_list), response['resourceIdentifiers']) if x is not None]

def main():
    log(logging.INFO, "===========================")
    resource_list = []
    list_resources_by_configservice(resource_list)
    print(resource_list)

if __name__ == '__main__':
    main()
