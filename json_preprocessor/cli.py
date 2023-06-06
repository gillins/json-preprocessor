import os
import boto3
#import click
import json
import fnmatch
from urllib.parse import urldefrag, urlsplit, urlparse
from .resolution import resolve

REGION = os.getenv('AWS_REGION')
if REGION is None:
    raise Exception('AWS_REGION not set')

# keyed on stack name
# values are a dict keyed on aws:cdk:path that contain the physical name
STACK_LOOKUP = {}

def retrieve_attribute(stack_name, path):
    """Retrieve an attribute for a CloudFormation resource using boto."""
    #print('retrieve_attribute', stack_name, path)

    global STACK_LOOKUP
    if stack_name not in STACK_LOOKUP:
        # hunt for it
        client = boto3.client('cloudformation', region_name=REGION)

        # get all the LogicalResourceId s
        ids = []
        paginator = client.get_paginator('list_stack_resources')
        for res in paginator.paginate(StackName=stack_name):
            for res2 in res['StackResourceSummaries']:
                ids.append(res2['LogicalResourceId'])

        stack = {}
        # now find all the paths
        for idn in ids:
            response = client.describe_stack_resource(StackName=stack_name, LogicalResourceId=idn)
            response = response['StackResourceDetail']
            physicalId = response['PhysicalResourceId']
            if 'Metadata' in response:
                response = json.loads(response['Metadata'])
                if 'aws:cdk:path' in response:
                    path = response['aws:cdk:path']
                    stack[path] = physicalId
                    #print(path, physicalId)

        # save
        STACK_LOOKUP[stack_name] = stack

    stack = STACK_LOOKUP[stack_name]

    has_wildcards = '*' in path or '?' in path or '[' in path
    if has_wildcards:
        result = []
        for candidate in stack:
            if fnmatch.fnmatch(candidate, path):
                result.append(stack[candidate])
        if len(result) == 0:
            raise Exception("pattern {} not found in stack {}".format(path, stack_name))
        return result

    else:
        if path in stack:
            return stack[path]
        else:
            raise Exception('path {} not found in stack {}'.format(path, stack_name))

def parse_cfn_uri(uri):
    """Parse a URI of the form:

       cfn://<stack-name>[@region]/<logical-name>[/[attribute]]

       [] denote optional components, whereas <> denotes mandatory components.

       If any mandatory components are missing, an exception will be raised.
    """

    # Deconstruct URI
    base_uri, frag = urldefrag(uri)
    base_uri_parts = urlsplit(base_uri)
    scheme = base_uri_parts.scheme

    # Check URI scheme
    if scheme != "cfn":
        raise Exception("Scheme '" + scheme + "' not supported.")

    return base_uri_parts.netloc, base_uri_parts.netloc + base_uri_parts.path

def handle_cfn_uri(uri):
    """Retrieve a stack resource attribute for a CloudFormation resource
       identified by a URI of the form:

       cfn://cdk_path
    """

    # Parse the URI
    stack_name, path = parse_cfn_uri(uri)

    return retrieve_attribute(stack_name, path)


def resolve_template_with_cfn_support(template_data, params):
    """Resolve a JSON-formatted CFN template and resolve any JSON References or
       pre-processor directives using the json_preprocessor library.

       An additional URI scheme handler is registered so that templates can
       reference pre-existing resources in existing CloudFormation stacks
       using a custom 'cfn://' scheme.
    """
    return resolve(template_data, params, {
        'cfn': handle_cfn_uri
    })


aws_profile_help_text = 'AWS profile to use when connecting to CloudFormation.'

minify_help_text = 'Compact the JSON output by removing whitespace.'

output_file_optional_help_text = 'Optional path to which JSON output will ' \
                                 'be written. By default output will be ' \
                                 'written to STDOUT.'

parameter_help_text = 'A key-value pair to be passed to the template; ' \
                      'this option may be used more than once to pass in ' \
                      'multiple key-value pairs.'

"""
@click.group(no_args_is_help=True,
             invoke_without_command=True)
@click.option('--minify',
              help=minify_help_text,
              is_flag=True,
              default=False)
@click.option('--output-file',
              metavar='<path>',
              help=output_file_optional_help_text,
              type=str,
              default=None)
@click.option('--parameter',
              metavar='<key=value>',
              help=parameter_help_text,
              type=str,
              multiple=True)
@click.argument('path-to-document',
                metavar='<path-to-document>',
                type=click.Path(exists=True))
"""
def run(minify, output_file, parameter, path_to_document):
    """Resolve a CloudFormation template containing JSON pre-processor
       directives.
    """
    param_dict = dict(param.split('=') for param in parameter)
    indent = None if minify else 4
    with open(path_to_document) as data:
        resolved_tree = resolve_template_with_cfn_support(json.load(data),
                                                          param_dict)
        resolved = json.dumps(resolved_tree, indent=indent)
        if output_file is None:
            print(resolved)
        else:
            with open(output_file, 'w') as f:
                f.write(resolved)
                f.close()
