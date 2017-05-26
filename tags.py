#!/usr/local/bin/python3
#
# tags.py v1.0
#
import json
import ast
import argparse
import subprocess as sp
import pprint
from pymongo import MongoClient

# global defaults
__script__ = 'tags.py'
__version__ = '1.0'
sd_list = {}
tmp = sp.call('clear',shell=True)
pp = pprint.PrettyPrinter(indent=4)

def sd_hosts(hosts):
    print('Server Discovery results...')
    print('')
    #print('isMaster.hosts:')
    #print hosts
    for cname in hosts:
        # print("cname:{}".format(cname))
        sd_list[cname] = {'cname':cname}
    print("Adding canonical names to Server discovery list:")
    pp.pprint(sd_list)
    print('')
    return

def sd_tags(hosts, mode): # server tags discovery function
    print('Tags Discovery results...')
    print('')
    for host in hosts:
        host_uri = 'mongodb://'+host+'/'
        host_conn = MongoClient(host_uri)
        host_im = host_conn.admin.command('ismaster')
        host_conn.close()
        #im_tags = ast.literal_eval(json.dumps(host_im['tags']))
        im_tags = host_im['tags'] # unicode
        print('isMaster.tags:', host)
        print(json.dumps(im_tags, indent=4))
        existing_host = sd_list[host]
        if mode == 'auto':
            print('tags_mode = auto, adding all tags to discovery list:')
            existing_host.update(im_tags)
            sd_list[host] = existing_host
        elif mode == 'public':
            print("tags_mode = public, adding 'int-public' tags to discovery list:")
            existing_host.update({k:v for k, v in im_tags.items() if k == 'int-public'})
            sd_list[host] = existing_host
        elif mode == 'private':
            print("tags_mode = private, adding 'int-private' tags to discovery list:")
            existing_host.update({k:v for k, v in im_tags.items() if k == 'int-private'})
            sd_list[host] = existing_host
        elif mode == 'backup':
            print("tags_mode = backup, adding 'int-backup' tags to discovery list:")
            existing_host.update({k:v for k, v in im_tags.items() if k == 'int-backup'})
            sd_list[host] = existing_host
        else:
            print('tags_mode = other, ignoring all tags')
        print('')
        #pprint(sd_list)
    print('')
    input("Press Enter to continue...")
    tmp = sp.call('clear',shell=True)
    print('Final Server Discovery description:')
    print('')
    pp.pprint(sd_list)
    print('')

def main():
    'main routine'
    # parse cli options
    parser = argparse.ArgumentParser(prog=__script__, description='tags.py version '+__version__, \
    epilog='A skunkworks PoC of tag aware server discovery process', \
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--connect', dest='uri', metavar='<connection_uri>', \
    help='MongoDB style connection uri')
    parser.add_argument('-t', '--tags', dest='tags', choices=['auto', 'public', 'private', 'backup', 'none'], \
    default='auto', metavar=('<auto> | <public> | <private> | <backup>'), help='topology tags')
    args = parser.parse_args()
    if args.uri is None:
        mongo_uri = 'mongodb://skunk01:27017,skunk01:27018,skunk01:27019/foo?replicaset=rs0'
    else:
        mongo_uri = args.uri
    print("Connection URI:", mongo_uri)
    if args.tags is None:
        tags_mode = 'auto'
    else:
        tags_mode = args.tags
    print("tags_mode:", tags_mode)
    print('')
    print('Connecting to replSet:', mongo_uri)
    input("Press Enter to continue...")
    tmp = sp.call('clear',shell=True)
    conn = MongoClient(mongo_uri)
    res_im = conn.admin.command('ismaster')
    conn.close()
    nodes = ast.literal_eval(json.dumps(res_im['hosts']))
    #print nodes
    sd_hosts(nodes)
    input("Press Enter to continue...")
    tmp = sp.call('clear',shell=True)
    sd_tags(nodes, tags_mode)

if __name__ == '__main__':
    main()

# EOF
