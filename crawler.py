import os
from urllib import parse
import multiprocessing
import time
import tldextract
import argparse
import csv
import mmap
import json
import shutil
import collections

MAX_PROCESSES = 1
MAX_SPECIFIC_PROCESSES = 1


def crawl(target=None, csv=None, site=None, rules=None):
    if target == "specific":
        if csv:
            crawl_specific_from_csv(csv)

        elif site and rules:
            print(site)
            print(rules)
            global base_directory
            base_directory = os.getcwd()

            crawl_specific_site([site, rules])

        else:
            print("error in call")
            parser.error("Setting target to specific requires --site and --rules.")

    # else:
    #   to_crawl_files = ''
    #   if args.target == 'test':
    #     to_crawl_files = "test_crawl/output_1.csv"
    #   elif args.target == 'all':
    #     to_crawl_files = "to_crawl/to-crawl_10k.csv"
    #   crawl_from_file(to_crawl_files)


def main():
    parser = argparse.ArgumentParser(
        prog="Breakage Crawler Wrapper",
        description="Crawler wrapper for the breakage crawler",
    )
    parser.add_argument(
        "-t",
        "--target",
        help="Target files to crawl test / all / specific",
        choices=["test", "all", "specific"],
        required=True,
    )
    parser.add_argument(
        "-s",
        "--site",
        help="Specific site to be crawled if the target is set to specific",
    )
    parser.add_argument(
        "-r",
        "--rules",
        help="Rules for specific link decoration to be blocked, given as a comma separated list",
    )
    parser.add_argument(
        "-c",
        "--csv",
        help="Name of the CSV if desired to be used for specific site checking",
    )

    args = parser.parse_args()

    if args.target == "specific":
        if args.csv and args.site:
            crawl_specific_from_csv_single_site(args.csv, args.site)
        
        elif args.csv:
            crawl_specific_from_csv(args.csv)

        elif args.site and args.rules:
            print(args.site)
            print(args.rules)
            global base_directory
            base_directory = os.getcwd()

            crawl_specific_site([args.site, args.rules])

        else:
            parser.error("Setting target to specific requires --site and --rules.")

    else:
        to_crawl_files = ""
        if args.target == "test":
            to_crawl_files = "test_crawl/output_1.csv"
        elif args.target == "all":
            to_crawl_files = "to_crawl/to-crawl_10k.csv"
        crawl_from_file(to_crawl_files)


def combine_key_values(dictionary):
    result = []
    for key, value in dictionary.items():
        result.append([key] + list(value))
    return result

def crawl_specific_from_csv_single_site(csv_file_name, site_name):
    base_folder_name = f'{csv_file_name.split(".")[0]}_data'
    urls_and_rule_to_crawl = collections.defaultdict(set)
    with open(csv_file_name, "r") as f:
        csv_reader = csv.reader(f)
        count = 0
        for site_url, decoration_name, rank in csv_reader:
            if count == 0:
                count += 1
                continue
            if site_name in site_url:
                urls_and_rule_to_crawl[site_url].add(decoration_name)

    if not os.path.isdir(base_folder_name):
        os.mkdir(base_folder_name)

    os.chdir(base_folder_name)

    global base_directory
    base_directory = os.getcwd()

    p = multiprocessing.Pool(MAX_SPECIFIC_PROCESSES)
    p.map(crawl_specific_site_conductor, combine_key_values(urls_and_rule_to_crawl))


def crawl_specific_from_csv(csv_file_name):
    base_folder_name = f'{csv_file_name.split(".")[0]}_data'
    urls_and_rule_to_crawl = collections.defaultdict(set)
    with open(csv_file_name, "r") as f:
        csv_reader = csv.reader(f)
        count = 0
        for site_url, decoration_name, rank in csv_reader:
            if count == 0:
                count += 1
                continue
            urls_and_rule_to_crawl[site_url].add(decoration_name)

    if not os.path.isdir(base_folder_name):
        os.mkdir(base_folder_name)

    os.chdir(base_folder_name)

    global base_directory
    base_directory = os.getcwd()

    p = multiprocessing.Pool(MAX_SPECIFIC_PROCESSES)
    p.map(crawl_specific_site_conductor, combine_key_values(urls_and_rule_to_crawl))


def check_for_block_in_log(path, specific_folder):
    if os.path.isdir(f"{path}/{specific_folder}") and log_name in os.listdir(path):
        with open(log_name, "rb", 0) as f, mmap.mmap(
            f.fileno(), 0, access=mmap.ACCESS_READ
        ) as s:
            if s.find(b"---------------------------------------------------") != -1:
                return True


def crawl_site(data_folder_name, log_name):
    log_name = f"{tld_string}-log"
    specific_folder_name = f"specific-data"

    crawl_succeeded = check_for_block_in_log(os.getcwd(), data_folder_name)

    cur_attempts = 0

    while cur_attempts < 5 and not crawl_succeeded:
        cur_attempts += 1
        print(
            f"{tld_string} - Attempt {cur_attempts} / 5 - On folder {specific_folder_name}"
        )
        rules_string = ",".join(rules)

        print(
            f"npm run crawl -- -u \"{url_to_crawl}\" -o {os.getcwd()}/{specific_folder_name} -v -f -q specific -s '{rules_string}' >> {os.getcwd()}/{log_name}"
        )

        time.sleep(20)

        os.system(
            f"npm run crawl -- -u \"{url_to_crawl}\" -o {os.getcwd()}/{specific_folder_name} -v -f -q specific -s '{rules_string}' >> {os.getcwd()}/{log_name}"
        )
        files = os.listdir(f"{os.getcwd()}/{specific_folder_name}")

        crawl_succeeded = check_for_block_in_log(os.getcwd(), data_folder_name)
        for file in files:
            if tld_string in file:
                with open(log_name, "r") as f, mmap.mmap(
                    f.fileno(), 0, access=mmap.ACCESS_READ
                ) as s:
                    if (
                        s.find(b"---------------------------------------------------")
                        != -1
                    ):
                        crawl_succeeded = True
                    break

    if not crawl_succeeded:
        # to_delete = f'{os.getcwd()}/{specific_folder_name}'
        os.chdir(base_directory)
        # if os.path.isdir(to_delete):
        # shutil.rmtree(to_delete, ignore_errors=True)
        return


def crawl_specific_site_conductor(url_and_rule_to_crawl):
    # print(url_and_rule_to_crawl)
    error_found = False

    url_to_crawl = url_and_rule_to_crawl[0]
    rules = url_and_rule_to_crawl[1:]

    print(rules)

    tld_obj = tldextract.extract(url_to_crawl)
    tld_string = tld_obj.domain + "." + tld_obj.suffix

    if "" in rules and len(rules) == 1:
        log_name = f"{tld_string}-log"
        specific_folder_name = f"specific-data"

        if not os.path.isdir(tld_string):
            try:
                os.mkdir(tld_string)
            except FileExistsError as e:
                print(e)

        os.chdir(tld_string)

        cur_folder = 0
        cur_attempts = 0
        crawls_since_last_success = 0

        while not cur_folder and cur_attempts < 1:
            # print(os.getcwd())
            print(f"{tld_string} - Attempt {cur_attempts} / 5 - On folder {cur_folder}")

            if crawls_since_last_success > 4:
                break

            cur_attempts += 1
            crawls_since_last_success += 1
            folder_name = f"specific-data"
            already_have_cur_folder = False

            if os.path.isdir(f"{os.getcwd()}/{folder_name}"):
                files = os.listdir(f"{os.getcwd()}/{folder_name}")
                for file in files:
                    if tld_string in file:
                        cur_folder += 1
                        crawls_since_last_success = 0
                        already_have_cur_folder = True
                        print("Found blank specific data folder pre-existing")
                        break

            if already_have_cur_folder:
                break

            # print(folder_name)
            os.system(
                f'npm run crawl -- -u "{url_to_crawl}" -o {os.getcwd()}/{folder_name} -v -f -q none >> {os.getcwd()}/{log_name}'
            )
            try:
                files = os.listdir(f"{os.getcwd()}/{folder_name}")
            except FileNotFoundError as e:
                print(f"Error in running blank crawl - {e}")
                os.chdir(base_directory)

                return
            for file in files:
                if tld_string in file:
                    cur_folder += 1
                    break

        if cur_attempts == 5:
            # to_delete = f'{os.getcwd()}/{folder_name}'
            os.chdir(base_directory)
            # if os.path.isdir(to_delete):
            # shutil.rmtree(to_delete, ignore_errors=True)
            return
    else:
        if "" in rules:
            rules.remove("")
        target_site_for_rule = [rule.split("||")[0] for rule in rules]

        unix_friendly_rules = ["-".join(rule.split("|")) for rule in rules]

        if ";" in unix_friendly_rules:
            os.chdir(base_directory)

        output_folder_name = f"{url_to_crawl}-{target_site_for_rule}"

        # print(os.listdir(os.getcwd()))
        # print(f'Current working: {os.getcwd()}')

        if not os.path.isdir(tld_string):
            # print(f'Making dir {url_to_crawl}')
            try:
                os.mkdir(tld_string)
            except FileExistsError as e:
                print(e)
                # time.sleep(10)

        os.chdir(tld_string)

        try:
            # tld_obj = tldextract.extract(url_to_crawl)
            # tld_string = tld_obj.domain + '.' + tld_obj.suffix

            log_name = f"{tld_string}-log"
            specific_folder_name = f"specific-data"

            crawl_succeeded = False

            if os.path.isdir(
                f"{os.getcwd()}/{specific_folder_name}"
            ) and log_name in os.listdir(os.getcwd()):
                with open(log_name, "rb", 0) as f, mmap.mmap(
                    f.fileno(), 0, access=mmap.ACCESS_READ
                ) as s:
                    if (
                        s.find(b"---------------------------------------------------")
                        != -1
                    ):
                        crawl_succeeded = True
                        print("Found pre-existing specific data folder")

            cur_attempts = 0
            while cur_attempts < 1 and not crawl_succeeded:
                cur_attempts += 1
                print(
                    f"{tld_string} - Attempt {cur_attempts} / 5 - On folder {specific_folder_name}"
                )

                rules_string = ",".join(rules)
                # rules_string = rules_string.replace("|", "\|")

                # print(rules_string)
                # time.sleep(30)

                os.system(
                    f'npm run crawl -- -u \"{url_to_crawl}\" -o {os.getcwd()}/{specific_folder_name} -v -f -q specific -s \"{rules_string}\" >> {os.getcwd()}/{log_name}'
                )
                files = os.listdir(f"{os.getcwd()}/{specific_folder_name}")
                for file in files:
                    if tld_string in file:
                        with open(log_name, "r") as f, mmap.mmap(
                            f.fileno(), 0, access=mmap.ACCESS_READ
                        ) as s:
                            if (
                                s.find(
                                    b"---------------------------------------------------"
                                )
                                != -1
                            ):
                                crawl_succeeded = True
                            break

            if not crawl_succeeded:
                # to_delete = f'{os.getcwd()}/{specific_folder_name}'
                os.chdir(base_directory)
                # if os.path.isdir(to_delete):
                #   shutil.rmtree(to_delete, ignore_errors=True)
                return

        except (FileNotFoundError, OSError) as e:
            error_found = True
            output_folder_name = f"{url_to_crawl}"

            error_site = f"{os.getcwd()}/{output_folder_name}"

    cur_control_folder = 0
    cur_control_attempts = 0
    crawls_since_last_success = 0

    folder_names = []

    while cur_control_folder < 5 and cur_control_attempts < 12:
        # print(os.getcwd())
        print(
            f"{tld_string} - Attempt {cur_control_attempts} / 12 - On folder {cur_control_folder}"
        )
        if cur_control_folder == 0 and cur_control_attempts == 5:
            break

        if crawls_since_last_success > 4:
            break

        cur_control_attempts += 1
        crawls_since_last_success += 1
        folder_name = f"control-data-{cur_control_folder}"
        already_have_cur_control_folder = False

        if os.path.isdir(f"{os.getcwd()}/{folder_name}"):
            files = os.listdir(f"{os.getcwd()}/{folder_name}")
            for file in files:
                if tld_string in file:
                    cur_control_folder += 1
                    crawls_since_last_success = 0
                    folder_names.append(folder_name)
                    already_have_cur_control_folder = True
                    print("Found control folder pre-existing")
                    break

        if already_have_cur_control_folder:
            continue

        # print(folder_name)
        os.system(
            f'npm run crawl -- -u "{url_to_crawl}" -o {os.getcwd()}/{folder_name} -v -f -q none >> {os.getcwd()}/{log_name}'
        )
        try:
            files = os.listdir(f"{os.getcwd()}/{folder_name}")
        except FileNotFoundError as e:
            print(f"Error in running control crawl - {e}")
            os.chdir(base_directory)

            return
        for file in files:
            if tld_string in file:
                cur_control_folder += 1
                crawls_since_last_success = 0
                folder_names.append(folder_name)
                break

    os.chdir(base_directory)
    if error_found:
        with open("specific-crawl-errors.txt", "a") as f:
            f.write(f"{error_site}\n")

    time.sleep(2)


def crawl_from_file(file):
    cur_folder = os.path.dirname(__file__)
    to_crawl_files = file
    crawl_file_path = os.path.join(cur_folder, to_crawl_files)
    urls_to_crawl = []
    with open(crawl_file_path) as f:
        lines = f.readlines()
        for line in lines:
            urls_to_crawl.append(line.rstrip("\n"))

    p = multiprocessing.Pool(MAX_PROCESSES)
    p.map(crawl_sites, urls_to_crawl)


def crawl_sites(url_to_crawl):
    tld_obj = tldextract.extract(url_to_crawl)
    tld_string = tld_obj.domain + "." + tld_obj.suffix

    # print(tld_string)
    #
    methods = {"all": False, "third": False, "replace": False}
    run_control_on_cur_site = False

    for method in methods.keys():
        cur_attempts = 0
        folder_name = f"{method}-data"
        while cur_attempts < 2 and not methods[method]:
            cur_attempts += 1
            os.system(
                f'npm run crawl -- -u "{url_to_crawl}" -o ./{folder_name} -v -f -q {method} >> log'
            )
            files = os.listdir(f"./{folder_name}")
            for file in files:
                if tld_string in file:
                    methods[method] = True
                    run_control_on_cur_site = True
                    break

    if run_control_on_cur_site:
        cur_control_folder = 0
        cur_control_attempts = 0
        crawls_since_last_success = 0

        while cur_control_folder < 5 and cur_control_attempts < 25:
            print(
                f"{tld_string} - Attempt {cur_control_attempts} / 20 - On folder {cur_control_folder}"
            )
            if cur_control_folder == 0 and cur_control_attempts == 5:
                break

            if crawls_since_last_success > 4:
                break

            cur_control_attempts += 1
            crawls_since_last_success += 1
            folder_name = f"control-data-{cur_control_folder}"
            os.system(
                f'npm run crawl -- -u "{url_to_crawl}" -o ./{folder_name} -v -f -q none >> log'
            )
            files = os.listdir(f"./{folder_name}")
            for file in files:
                if tld_string in file:
                    cur_control_folder += 1
                    crawls_since_last_success = 0
                    break

    time.sleep(1)


if __name__ == "__main__":
    main()
