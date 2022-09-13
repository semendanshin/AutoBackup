import aiohttp
from infi.devicemanager import DeviceManager
from pprint import pprint
import os
import sys
import posixpath
import asyncio
import yadisk_async
import json
import deepdiff
from datetime import datetime


# github.com/firs-iln

async def get_token(client_id, secret):pip
    async with yadisk_async.YaDisk(client_id, secret) as y:
        url = y.get_code_url()

        print("Go to the following url: %s" % url)
        code = input("Enter the confirmation code: ")

        try:
            response = await y.get_token(code)
        except yadisk_async.exceptions.BadRequestError:
            print("Bad code")
            sys.exit(1)

        y.token = response.access_token

        if await y.check_token():
            print("Successfully received token!")
        else:
            print("Something went wrong. Not sure how though...")
        return y.token


ClientID = os.getenv("YADISK_CLIENTID")
Secret = os.getenv("YADISK_SECRET")
TOKEN = os.getenv("YADISK_TOKEN")
if not TOKEN:
    loop = asyncio.get_event_loop()
    TOKEN = loop.run_until_complete(get_token(ClientID, Secret))
    os.system(f'setx YADISK_TOKEN "{TOKEN}"')
    print("You should restart your computer before next running the program")


YADISK_START_DIR = '/SSD_Backup/'
LOCAL_START_DIR = 'H:/'


async def upload_files(queue, disk):
    while queue:
        in_path, out_path = queue.pop(0)

        print("Uploading %s -> %s" % (in_path, out_path))

        try:
            await disk.upload(in_path, out_path)
        except yadisk_async.exceptions.PathExistsError:
            print("%s already exists" % (out_path,))
        except aiohttp.ServerTimeoutError:
            print(f"Failed uploading {in_path}")
            queue.append((in_path, out_path))


async def create_dirs(queue, disk):
    while queue:
        path = queue.pop(0)

        print("Creating directory %s" % (path,))

        try:
            await disk.mkdir(path)
        except yadisk_async.exceptions.PathExistsError:
            print("%s already exists" % (path,))


async def make_recursive_dir_copy(from_dir, to_dir, n_parallel_requests=5):
    loop = asyncio.get_event_loop()
    async with yadisk_async.YaDisk(ClientID, Secret, TOKEN) as y:
        try:
            mkdir_queue = []
            upload_queue = []

            print("Creating directory %s" % (to_dir,))

            try:
                loop.run_until_complete(y.mkdir(to_dir))
            except yadisk_async.exceptions.PathExistsError:
                print("%s already exists" % (to_dir,))

            for root, dirs, files in os.walk(from_dir):
                rel_dir_path = root.split(from_dir)[1].strip(os.path.sep)
                rel_dir_path = rel_dir_path.replace(os.path.sep, "/")
                dir_path = posixpath.join(to_dir, rel_dir_path)

                for dir_name in dirs:
                    mkdir_queue.append(posixpath.join(dir_path, dir_name))

                for filename in files:
                    out_path = posixpath.join(dir_path, filename)
                    rel_dir_path_sys = rel_dir_path.replace("/", os.path.sep)
                    in_path = os.path.join(from_dir, rel_dir_path_sys, filename)

                    upload_queue.append((in_path, out_path))

                tasks = [upload_files(upload_queue, y) for i in range(n_parallel_requests)]
                tasks.extend(create_dirs(mkdir_queue, y) for i in range(n_parallel_requests))

                loop.run_until_complete(asyncio.gather(*tasks))
        finally:
            loop.run_until_complete(y.close())


def make_current_file_tree(start_dir):
    def scan_dir(start_path):
        folder_items = dict()
        for entry in os.scandir(start_path):
            if entry.is_dir():
                folder_items[entry.name] = {'content': scan_dir(entry.path)}
            else:
                folder_items[entry.name] = [entry.stat().st_size, entry.stat().st_mtime]
        return folder_items

    return scan_dir(start_dir)


async def make_differential_copy(start_dir, to_dir, n_parallel_requests=5):
    files_to_copy = parse_differences(find_difference(start_dir))
    pprint(files_to_copy)
    print(sorted(files_to_copy.keys(), key=lambda x: x.count('/')))
    with open('tmp/difference.json', 'w', encoding='UTF-8') as f:
        f.write(json.dumps(files_to_copy))
    copy_name = f"BackupCopy_{str(datetime.now()).replace(' ', '_').replace(':', '-')}_differential"
    print(copy_name)
    to_dir = to_dir + copy_name + '/'
    print(to_dir)
    mkdir_queue = []
    upload_queue = []
    async with yadisk_async.YaDisk(ClientID, Secret, TOKEN) as y:
        mkdir_queue.append(to_dir)
        upload_queue.append(('tmp/difference.json', to_dir))
        mkdir_queue.append(to_dir + 'files/')
        for el in sorted(files_to_copy.keys(), key=lambda x: x.count('/')):
            match files_to_copy[el]:
                case 'add' | 'ch':
                    upload_queue.append((LOCAL_START_DIR + el, to_dir + 'files/' + el))
        pprint(mkdir_queue)
        pprint(upload_queue)


def find_difference(start_dir):
    current_tree = make_current_file_tree(start_dir)
    with open('last_full_copy.json') as f:
        last_file_tree = json.loads(f.readline())
    return deepdiff.DeepDiff(last_file_tree, current_tree)


def parse_differences(differences: deepdiff):
    def parse_keys(keys):
        return ['/'.join(x.replace("root['", '').replace("['", '').split("']")[0:-1:2]) for x in keys]

    parsed_differences = dict()

    if 'dictionary_item_added' in differences:
        for el in parse_keys(differences['dictionary_item_added']):
            parsed_differences[el] = 'add'
    if 'dictionary_item_removed' in differences:
        for el in parse_keys(differences['dictionary_item_removed']):
            parsed_differences[el] = 'rm'
    if 'values_changed' in differences:
        for el in parse_keys(differences['values_changed'].keys()):
            parsed_differences[el] = 'ch'
    return parsed_differences


def set_working_disk():
    dm = DeviceManager()
    dm.root.rescan()
    devices = dm.disk_drives
    print('\n'.join([str(i) + ' - ' + str(devices[i]) for i in range(len(devices))]))
    try:
        wanted_device = int(input('Write in number of the device, with will be used to make copies: '))
        if wanted_device >= len(devices):
            raise ValueError
    except ValueError:
        print('Wrong data type or number out of range')
        return
    return wanted_device


def main():
    asyncio.run(make_differential_copy(LOCAL_START_DIR, YADISK_START_DIR))
    # :
    #     make_full_copy(y, LOCAL_START_DIR, YADISK_START_DIR, 5)
    # with open('last_full_copy.json', 'w', encoding='UTF-8') as f:
    #     f.write(json.dumps(make_current_file_tree(LOCAL_START_DIR)))


if __name__ == '__main__':
    main()
