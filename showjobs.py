#!/hpf/tools/centos6/python/2.7.11/bin/python
################################################################################
#
# showjobs - list historical job information
#
# To enable job logging set the TORQUE server parameter record_job_info to TRUE
# Edit this file to set TORQUE_HOME_DIR to the Torque home directory
# Move or link to this file in a directory in your user's path
# Run showjobs --help for a brief help message or showjobs --man for a man page
#
################################################################################

import argparse
import collections
import time
import os
import glob
from datetime import datetime, timedelta

from lxml import etree


# Set TORQUE_HOME_DIR to the Torque home directory
TORQUE_HOME_DIR = '/opt/torque_job_logs'


# TODO: find a better solution?
def staticfunction(func):
    """
    Decorate func by treating func as a staticmethod,
      but return the actual callable

    :param func: function
    :return: function
    """
    return staticmethod(func).__func__


###############################################################################
# ELEMENT PARSER
###############################################################################

class ElementParser:
    """
    Class that contains utilities for extracting information from an lxml Element
    Call ElementParser.parse(element) on an lxml Element to return a dict of field name -> value

    Any fields that we can't find will not be defined in the result dict
    Use 'hasattr(dict, field)' to determine if we have found 'field'
    """
    @staticfunction
    def path_lookup(path):
        """
        Generate a parse function that will look up path in a given element and return the relevant text

        The parse function will return None if we can't find path
        :param path: xpath to look for in the element
        :return: function that parses an element to return a str
        """
        def parse_function(element):
            try:
                text_elem = element.find(path)
                if text_elem is not None:
                    return text_elem.text
            except:
                return None

        return parse_function

    @staticfunction
    def parse_var_list(var_name):
        """
        Generate a parse function that will parse the Variable_List in the given element for var_name

        The parse function will return None if we can't find var_name
        :param var_name: variable name to look for in Variable_List
        :return: function that parses an element to return a str
        """
        def parse_function(element):
            try:
                for key_value_pair in element.find('Variable_List').text.split(','):
                    key, value = key_value_pair.split('=')
                    if key == 'PBS_O_HOME':
                        return value
            except:
                return None

        return parse_function

    @staticfunction
    def parse_master_host(element):
        """
        Given an element, parse element's 'exec_host' child

        Return None if we can't find anything
        :param element: lxml Element
        :return: str
        """
        try:
            # Take everything before the '/'
            # Eg. r2a-1/4,9 -> r2a-1
            return element.find('exec_host').text.split('/')[0]
        except:
            return None

    """
    This is a mapping from each showjobs field -> function to parse an lxml element
    Eg. if we want to parse 'element' for 'Job Id', we would run:
      field_name_to_parse_function['Job Id'](element)
    This looks up the appropriate function for 'Job Id', and then uses it to parse 'element' for the required text
    Each parse function will return None if we can't find the required field
    """
    field_name_to_parse_function = dict([
        ('Job Id', path_lookup('Job_Id')),
        ('Job Name', path_lookup('Job_Name')),
        ('Output File', path_lookup('Output_Path')),
        ('Error File', path_lookup('Error_Path')),
        ('Submit Arguments', path_lookup('submit_args')),
        ('User Name', path_lookup('euser')),
        ('Group Name', path_lookup('egroup')),
        ('Account Name', path_lookup('comp_time')),
        ('Queue Name', path_lookup('queue')),
        ('Quality Of Service', path_lookup('Resource_List/qos')),
        ('Architecture', path_lookup('Resource_List/arch')),
        ('Operating System', path_lookup('Resource_List/opsys')),
        ('Wallclock Limit', path_lookup('Resource_List/walltime')),
        ('Wallclock Duration', path_lookup('resources_used/walltime')),
        ('CPUTime', path_lookup('resources_used/cput')),
        ('Memory Used', path_lookup('resources_used/mem')),
        ('Memory Limit', path_lookup('Resource_List/mem')),
        ('vmem Used', path_lookup('resources_used/vmem')),
        ('vmem Limit', path_lookup('Resource_List/vmem')),
        ('Submit Time', path_lookup('qtime')),
        ('Start Time', path_lookup('start_time')),
        ('End Time', path_lookup('comp_time')),
        ('Exit Code', path_lookup('exit_status')),
        ('Interactive', path_lookup('format_interactive')),
        ('Job Dependencies', path_lookup('depend')),
        ('Job Script', path_lookup('job_script')),
        ('Home Directory', parse_var_list('PBS_O_HOME')),
        ('Working Directory', parse_var_list('PBS_O_WORKDIR')),
        ('Master Host', parse_master_host)
    ])

    @classmethod
    def parse(cls, element):
        """
        Parse an element and return a dict mapping from display_dict name to value

        Note: If we can't find an attribute, then that attribute will be undefined in the result dict
        :param element: lxml Element
        :return: dict
        """
        dictionary = dict()

        for field_name, parse_method in cls.field_name_to_parse_function.items():
            parsed_result = parse_method(element)
            if parsed_result is not None:
                dictionary[field_name] = parsed_result

        return dictionary


###############################################################################
# ELEMENT DISPLAYER
###############################################################################

class ElementDisplayer:
    """
    Class that contains utilities for formatting and displaying a single 'showjobs' entry

    Call ElementDisplayer.display_dict(dictionary) on a dictionary with the 'showjobs' fields defined
      to return a string representation of this showjobs entry
    """
    @staticfunction
    def format_default(value):
        """Identity function"""
        return value

    @staticfunction
    def format_duration(seconds):
        """
        Format seconds as dd:hh:mm:ss

        Remove the 'dd:' part if the number of days == 0
        :param seconds: str
        :return: str
        """
        td = timedelta(seconds=int(seconds))
        return ('{:02d}:'.format(td.days) if td.days > 0 else '') + time.strftime('%H:%M:%S', time.gmtime(td.seconds))

    @staticfunction
    def format_mem(mem):
        """
        Format a memory size for display_dict

        Eg. 9373440kb -> 8.9Gb
        :param mem: str
        :return: str
        """
        if 'k' in mem.lower():
            # Get the numerical part of the memory
            # Eg. 123kb -> 123
            kilo = float(''.join(char for char in mem if str.isdigit(char)))
            if kilo > 1024:
                mega = kilo / 1024
                if mega > 1024:
                    giga = mega / 1024
                    return '%.1f' % giga + 'Gb'

                return '%.1f' % mega + 'Mb'

        return mem

    @staticfunction
    def format_date(date):
        """
        Return a string representation of a date given in seconds

        :param date: seconds as a string
        :return: str
        """
        # Example output:
        #   Wed Oct 26 22:37:02 2016
        return datetime.fromtimestamp(int(date)).strftime('%a %b %d %H:%M;%S %Y')

    @staticfunction
    def format_interactive(value):
        """
        If the 'Interactive' field
        The 'Interactive' field is always reported as 'True' if the field is given
        """
        return 'True'

    """
    This is a mapping from each showjobs field -> algorithm to format this field
    Eg. if we want to format the 'Memory Used' field, we would run the following
      field_name_to_format_function['Memory Used']('1000Kb')
    This looks up the appropriate formatting function for 'Memory Used', and then runs it on the value for this field

    Each parsing function will return the required representation of its particular field

    It is worth noting that most of the fields do not require formatting,
      and thus map to the 'format_default' function, which does nothing

    Note that all fields will be displayed in the order they are given here
    """
    field_name_to_format_function = collections.OrderedDict([
        ('Job Id', format_default),
        ('Job Name', format_default),
        ('Output File', format_default),
        ('Error File', format_default),
        ('Working Directory', format_default),
        ('Home Directory', format_default),
        ('Submit Arguments', format_default),
        ('User Name', format_default),
        ('Group Name', format_default),
        ('Account Name', format_default),
        ('Queue Name', format_default),
        ('Quality Of Service', format_default),
        ('Architecture', format_default),
        ('Operating System', format_default),
        ('Node Count', format_default),
        ('Wallclock Limit', format_duration),
        ('Wallclock Duration', format_duration),
        ('CPUTime', format_duration),
        ('Memory Used', format_mem),
        ('Memory Limit', format_mem),
        ('vmem Used', format_mem),
        ('vmem Limit', format_mem),
        ('Submit Time', format_date),
        ('Start Time', format_date),
        ('End Time', format_date),
        ('Exit Code', format_default),
        ('Master Host', format_default),
        ('Interactive', format_interactive),
        ('Job Dependencies', format_default),
        ('Job Script', format_default),
    ])

    @classmethod
    def format_field(cls, field, value):
        """
        Format the given value

        Lookup the formatter in field_name_to_format_function, and run it on the input
        :param field: field name
        :param value: field value
        :return: str
        """
        return cls.field_name_to_format_function[field](value)

    @classmethod
    def format_dict(cls, dictionary):
        """
        Format the given dictionary for use in display_dict

        For each field in the dictionary, format the field and return a new dictionary,
          where all of the fields are formatted
        :param dictionary: dict
        :return: dict
        """
        for key, value in dictionary.items():
            dictionary[key] = cls.format_field(key, value)

        return dictionary

    @classmethod
    def order_dict(cls, dictionary):
        """
        Return a OrderedDict created in the order that the showjobs entry will be displayed

        When iterating over the result, the fields will iterate in the order they should be displayed
        :param dictionary: dict
        :return: collections.OrderedDict
        """
        # To get the order for the fields, look up the keys from field_name_to_format_function
        return collections.OrderedDict((field, dictionary[field]) for field in cls.field_name_to_format_function.keys()
                                       if field in dictionary)

    @classmethod
    def display_field(cls, field, value):
        """
        Return a string representation of value

        Requires that value has been previously formatted using format_field
        :param field: field name
        :param value: field value
        :return: str
        """
        return '{key: <18}: {value}\n'.format(key=field, value=value)

    @classmethod
    def display_dict(cls, dictionary):
        """
        Return a string representing this showjobs entry

        dictionary must map from 'showjobs' field -> raw text from lxml Element
        :param dictionary: dict
        :return: str
        """
        # Format the fields in dictionary
        dictionary = cls.order_dict(cls.format_dict(dictionary))

        return ''.join(cls.display_field(field, value) for field, value in dictionary.items()) + \
               '-' * 80 + '\n'


###############################################################################
# GET FILES
###############################################################################

def all_files():
    """
    Return a list of all job log files

    :return: list
    """
    return sorted(glob.glob(os.path.join(TORQUE_HOME_DIR, '20*')))


def get_file_list(args):
    """
    Return a list of all filenames that we are going to search through

    :param args: args from argparse
    :return: list
    """
    files = all_files()

    if args.days is not None:
        num_days = int(args.days)
        if num_days < len(files):
            # Take the last num_days files
            files = files[-num_days:]

    if args.start:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        files = [file for file in files if datetime.strptime(file, '%Y%m%d') > start_date]

    if args.end:
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        files = [file for file in files if datetime.strptime(file, '%Y%m%d') > end_date]

    return files


###############################################################################
# XPATH
###############################################################################

def get_xpath(args):
    """
    Given the user's command-line args, return an xpath string representing the user's request

    :param args: args from argparse
    :return: str
    """
    xpath = "."

    xpath += "[Job_Id='{}']".format(args.job_id) if args.job_id is not None else ''
    xpath += "[queue='{}']".format(args.queue) if args.queue is not None else ''
    xpath += "[egroup='{}']".format(args.group) if args.group is not None else ''
    xpath += "[Account_Name='{}']".format(args.account) if args.account is not None else ''
    xpath += "[euser='{}']".format(args.user) if args.user is not None else ''

    return xpath


###############################################################################
# PARSING
###############################################################################

def parse_file(file, xpath, one_only):
    """
    Parse the given file and return a list of lxml Elements

    :param file: filename
    :param xpath: xpath representing query
    :param one_only: whether to return a single result if possible
    :return: list
    """
    elements = []
    with open(file) as f:
        # Create a parser that tries hard to parse broken xml
        parser = etree.XMLParser(recover=True)

        for line in f:
            parser.feed(line)
            if line.startswith('</Jobinfo>'):
                element = parser.close().find(xpath)

                if element is not None: # We have found a match for our query
                    if one_only:
                        return [element]

                    elements.append(element)

        return elements


def parse_files(files, xpath, one_only=False):
    """
    Parse the given filenames and return a list of Elements

    :param files: list of filenames to parse
    :param xpath: xpath representing query
    :param one_only: whether to return a single result if possible
    :return: list
    """
    elements = []
    for file in files:
        elements.extend(parse_file(file, xpath, one_only))

        if one_only and len(elements) > 0:
            return elements

    return elements


###############################################################################
# MAIN
###############################################################################

def elements_to_str(elements):
    """
    Convert the lxml Elements to a string representation

    :param elements: list of lxml Elements
    :return: str
    """
    return '\n'.join(ElementDisplayer.display_dict(ElementParser.parse(e)) for e in elements)


def query_jobs(args):
    """
    Given the query args, look up the required jobs and return a string representation of the results

    :param args: args from argparse
    :return: str
    """
    files = get_file_list(args)
    xpath = get_xpath(args)

    elements = parse_files(files, xpath, args.one_only)

    return elements_to_str(elements)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-u', '--user')
    parser.add_argument('-g', '--group')
    parser.add_argument('-a', '--account')
    parser.add_argument('-q', '--queue')

    parser.add_argument('-s', '--start')
    parser.add_argument('-e', '--end')
    parser.add_argument('-n', '--days')

    parser.add_argument('-o', '--one-only', action='store_true')
    parser.add_argument('--man', action='store_true')
    parser.add_argument('-j', '--job-id')

    # TODO support --full option to print all fields

    return parser.parse_args()


def usage():
    return '''\
NAME
       showjobs - list historical job information

SYNOPSIS
       showjobs [-u user_name] [-g group_name] [-a account_name] [-q queue_name] [-s start
       date] [-e end date] [-n days] [-o|--oneonly] [--help] [--man] [[-j] <job id>]

DESCRIPTION
       The showjobs command is used to list past job information. It searches through the
       designated job files while filtering according to the specified options. The relevant
       fields for each job are shown in a multi-line format, with a blank line between jobs.

OPTIONS
       -a account_name
           Show only job records matching the specified account.

       -e end_date
           Restricts the search to job files ending with the specified date. The date is
           specified in the format YYYY-MM-DD. The default query searches to the latest
           available job file.

       -g group_name
           Show only job records matching the specified group.

       [-j] job_id
           Show only job records matching the specified job id.

       -n days
           Restricts the number of past job files to search.

       -q queue_name
           Show only job records matching the specified queue.

       -s start_date
           Restricts the search to job files starting with the specified date. The date is
           specified in the format YYYY-MM-DD. The default query searches from the earliest
           available job file.

       -u user_name
           Show only job records matching the specified user.

       -o | --oneonly
           Show only the first job record found. This will mostly be much faster and give the
           same result as if the flag is omitted if the search is for a specific non-array job
           or specific array job member.

       -h | --help
           brief help message

       --man
           full documentation

EXAMPLE
       Show job information for job id 220 and restrict the search to the last 4 days.

       showjobs -n 4 -j 220
'''


def main():
    args = parse_args()

    if args.man:
        print(usage())
        quit()
    else:
        print(query_jobs(args))


if __name__ == '__main__':
    main()