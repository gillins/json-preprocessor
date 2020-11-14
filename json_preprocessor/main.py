#!/usr/bin/env python
import boto.cloudformation
import click
import json
from urllib.parse import urldefrag, urlsplit
from .resolution import resolve


def retrieve_attribute(stack_name, logical_name, attribute, region):
    """Retrieve an attribute for a CloudFormation resource using boto."""

    if region is None:
        connection = boto.cloudformation.CloudFormationConnection()
    else:
        connection = boto.cloudformation.connect_to_region(region)

    response = connection.describe_stack_resource(stack_name, logical_name)

    # Unwrap DescribeStackResourceResponse
    dsr_response = response["DescribeStackResourceResponse"]

    # Unwrap DescribeStackResourceResult
    dsr_result = dsr_response["DescribeStackResourceResult"]

    # Unwrap StackResourceDetail
    sr_detail = dsr_result["StackResourceDetail"]

    return sr_detail[attribute]


def parse_cfn_uri(uri):
    """Parse a URI of the form:

       cfn://<stack-name>[@region]/<logical-name>[/[attribute]]

       [] denote optional components, whereas <> denotes mandatory components.

       If any mandatory components are missing, an exception will be raised.
    """

    # Deconstruct URI
    base_uri, frag = urlparse.urldefrag(uri)
    base_uri_parts = urlparse.urlsplit(base_uri)

    # Check URI scheme
    if base_uri_parts.scheme != "cfn":
        raise Exception("Scheme '" + base_uri_parts.scheme + "' not supported.")

    # Parse netloc section
    netloc_parts = base_uri_parts.netloc.split("@")
    if len(netloc_parts) > 2:
        raise Exception("URI contains unexpected components in net " +
                        "location '" + base_uri_parts.netloc + "'.")
    elif len(netloc_parts) >= 1:
        stack_name = netloc_parts[0]
        if len(netloc_parts) == 2:
            region = netloc_parts[1]
        else:
            region = None
    else:
        raise Exception('URI is missing stack name.')

    # Parse path section
    path_parts = base_uri_parts.path.strip("/").split('/')
    if len(path_parts) > 2:
        raise Exception("URI contains unexpected components in query " +
                        "path '" + base_uri_parts.path + "'.")
    elif len(path_parts) >= 1:
        logical_name = path_parts[0]
        if len(path_parts) == 2:
            attribute = path_parts[1]
        else:
            attribute = None
    else:
        raise Exception("URI is missing logical name for stack resource.")

    return stack_name, logical_name, attribute, region


def handle_cfn_uri(uri):
    """Retrieve a stack resource attribute for a CloudFormation resource
       identified by a URI of the form:

       cfn://<stack-name>[@region]/<logical-name>[/[attribute]]

       [] denote optional components, whereas <> denotes mandatory components.

       If any mandatory components are missing, an exception will be raised.

       If [region] is omitted, then the region will be determined using the
       current user's AWS credentials. If [region] is present, but not
       recognised by boto, an exception may be raised.

       [attribute] may be any attribute that can be returned by the retrieval
       function. If [attribute] is omitted, the 'PhysicalResourceID' attribute
       will be returned.
    """

    # Parse the URI
    stack_name, logical_name, attribute, region = parse_cfn_uri(uri)

    if attribute is None:
        attribute = 'PhysicalResourceID'

    return retrieve_attribute(stack_name, logical_name, attribute, region)


def resolve_template_with_cfn_support(template_data, params):
    """Resolve a JSON-formatted CFN template and resolve any JSON References or
       pre-processor directives using the json_preprocessor library.

       An additional URI scheme handler is registered so that templates can
       reference pre-existing resources in existing CloudFormation stacks
       using a custom 'cfn://' scheme.
    """
    return resolve(json.load(template_data), params, {
        'cfn': handle_cfn_uri
    })


aws_profile_help_text = 'AWS profile to use when connecting to CloudFormation.'

minify_help_text = 'Compact the JSON output by removing whitespace.'

output_file_optional_help_text = 'Optional path to which JSON output will be ' \
                                 'written. By default output will be written ' \
                                 'to STDOUT.'

parameter_help_text = 'A key-value pair to be passed to the template; ' \
                      'this option may be used more than once to pass in ' \
                      'multiple key-value pairs.'


@click.group(no_args_is_help=True,
             invoke_without_command=True)
@click.option('--minify',
              help=minify_help_text,
              is_flag=True,
              default=False)
@click.option('--output-file', metavar='<path>',
              help=output_file_optional_help_text,
              type=str, default=None)
@click.option('--parameter', metavar='<key=value>',
              help=parameter_help_text,
              type=str, multiple=True)
@click.argument('path-to-document', metavar='<path-to-document>',
                type=click.Path(exists=True))
def cli(minify, output_file, parameter, path_to_document):
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


if __name__ == '__main__':
    cli()
