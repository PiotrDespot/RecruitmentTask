##!/usr/bin/env python

import sys


TYPE = ("inner", "left", "right")
DEFAULT_TYPE = "inner"


class InputError(Exception):
    """
        Exception raised when input does not match the requested command
    """
    pass


def look_for_duplicates(file_name1, file_name2, merge_column):
    """

    :param file_name1: directory of 1st file
    :param file_name2: directory of 2nd file
    :param merge_column: merge type
    :return: list of duplicates

    This function searches for duplicate columns in two files we are about to merge.
    It also checks if the column we use to merge both files exists in both files.
    """
    column_duplicates = []
    try:
        # checking if merge column exists in 1st and 2nd file:
        with open(file_name1, 'r') as f1:
            header1 = f1.readline().replace("\n", "").split(",")
            if not (merge_column in header1):
                raise InputError
        with open(file_name2, 'r') as f2:
            header2 = f2.readline().replace("\n", "").split(",")
            if not (merge_column in header1):
                raise InputError

        # looking for duplicates and making a list with indexes of all duplicates.
        for column in header1:
            if column in header2:
                if column == merge_column:
                    duplicate_found = [header1.index(column), header2.index(column)]
                else:
                    duplicate_found = [header1.index(column), header2.index(column)]
                column_duplicates.append(duplicate_found)

        return column_duplicates
    except IOError:
        print("Couldn't access file")
        sys.exit([3])
    except InputError:
        print("One of the files doesnt have the column specified in the input.")
        sys.exit([3])
    except Exception:
        print("An error occurred while searching for matching columns")
        sys.exit([3])


def merge_files(file_name1, file_name2, duplicate_list, merge_type):
    """
    _______________________________________________________________________\n
    This function is used for merging both files\n
    param file_name1: directory of the 1st file\n
    param file_name2: directory of the 2nd file\n
    param duplicate_list: list of duplicate columns\n
    param merge_type: merge type\n
    return: yields merged file line after line\n
    ____________________________________________________________________________\n
    This function opens both files,reads then loops through them.
    For every line in 1st file it will search for a matching value in
    columns that are repeating in both files. If it finds one, it will remove
    it from the line of the 2nd file. All matching duplicates are removed it
    will append the 2nd line to the 1st file and yield the result.
    If the merge type is "inner" the function will only yield lines that
    fully match each other.
    If the merge type is "left" or "right", the merge will add NaN to the
    columns that don't match each other.\n
    Example:\n
    If we have two csv files:\n
    ****************************************\n
    Id,name,age        name,age,height\n
    1,piotr,19         piotr,19,180\n
    2,pawel,18         pawel,20,190\n
    ****************************************\n
    merge type "inner" will return only:\n
    ****************************************\n
    Id,name,age,height\n
    1,piotr,19,180\n
    ****************************************\n
    because age doesnt match between pawel from 1st and 2nd file, thus these are
    different people - they shouldn't be merged. Therefore "left" will yield:\n
    ****************************************\n
    Id,name,age,height\n
    1,piotr,19,180\n
    2,pawel,18,NaN\n
    ****************************************\n
    And "right" will yield:\n
    ****************************************\n
    name,age,height,Id\n
    piotr,19,180,1\n
    pawel,20,190,NaN\n
    ****************************************\n
    ________________________________________________________________________________
    """
    try:
        if merge_type == "right":
            # if merge type is "right", swap the order of reading files.
            file_name2, file_name1 = file_name1, file_name2
        with open(file_name1, 'r') as f1: # open 1st file
            for line1 in f1:
                # 1st loop:
                # for every line in 1st file, open the 2nd file, find matching row
                # remove column from 2nd file if values from all duplicates in one line match
                with open(file_name2, 'r') as f2:
                    for line2 in f2:
                        # 2nd loop:
                        # make a list from a line for both files
                        line_list1 = line1.replace("\n", "").split(",")
                        line_list2 = line2.replace("\n", "").split(",")
                        i = 0
                        for index_file1, index_file2 in duplicate_list:
                            # 3rd loop:
                            # check if the values in every duplicate match
                            if merge_type == "right":
                                # if merge type is "right", swap index order of duplicates
                                index_file2, index_file1 = index_file1, index_file2
                            index_file2 = index_file2 - i

                            if line_list1[index_file1] == line_list2[index_file2]:
                                # if values in the columns match, remove value from 2nd file
                                line_list2.remove(line_list2[index_file2])
                                i += 1
                            else:
                                # if any of the values do not match, break the 3rd loop, the line will be skipped
                                break
                        else:
                            # if all the values match, add remaining values to the line of the 1st file
                            line_list1.extend(line_list2)
                            final_row = line_list1.copy()
                            if merge_type == "inner":
                                # if the merge type is "inner", we will yield a new string that is the
                                # merged row
                                yield ",".join(final_row)
                            # we found the matching lines, so we can break the 2nd loop
                            break
                    else:
                        # this will execute if none of the lines match
                        if merge_type == "left" or merge_type == "right":
                            line_list1 = line1.replace("\n", "").split(",")
                            line_list2 = line2.replace("\n", "").split(",")
                            i = 0
                            for index_file1, index_file2 in duplicate_list:
                                # remove values from duplicates in 2nd file
                                if merge_type == "right":
                                    index_file2, index_file1 = index_file1, index_file2
                                index_file2 -= i
                                line_list2.remove(line_list2[index_file2])
                                i += 1
                            else:
                                # add NaN to every remaining column
                                line_list2 = ["NaN" for column in line_list2]
                                line_list1.extend(line_list2)
                                final_row = line_list1.copy()
                        else:
                            pass
                    if merge_type == "left" or merge_type == "right":
                        # yield the result that is the merged line
                        yield ",".join(final_row)
    except Exception:
        print("An error occurred while merging files.")


def user_input():
    """
    -------------------------------------------------------------------------------------------------\n
                Proper input looks like this:\n
                join file_path file_path column_name join_type
    -------------------------------------------------------------------------------------------------\n
            This function looks for errors in arguments.
            The column type should be written in quotation if it has a whitespace in its name.
    --------------------------------------------------------------------------------------------------\n
    :return: input_command - function will return elements of the list:
             file_path1 - input_command[0]
             file_path2 - input_command[1]
             column_name - input_command[2]
             join_type - input_command[3]
    """
    try:
        command = ' '.join(sys.argv[1:])  # All the arguments are joined into a string
        input_command = command.split('"')  # The user input is split into a list
        input_command = [word.strip(" ") for word in input_command]  #removing extra whitespaces
        formatted_input1 = input_command[0].split(" ")  # splitting the string
        formatted_input2 = input_command[1:]

        formatted_input1.extend(formatted_input2)
        input_command = formatted_input1

        if len(input_command) < 3:
            # input needs to have at least 3 arguments.
            raise IndexError
        elif len(input_command) == 3:
            # if input has 4 arguments, we will add default merge type to the end of a list.
            input_command.append(DEFAULT_TYPE)
        elif len(input_command) == 4 and input_command[-1] not in TYPE:
            # merge type needs to be the last argument in a list
            raise InputError
        elif len(input_command) > 4:
            # we cant have more than 4 arguments in a list
            raise InputError
    except InputError:
        print("""
        Wrong input!\n
        Correct input should look like this: join file_path file_path column_name join_type\n
        If the column type has white space in its name, write it in quotation.\n
        """)
        sys.exit([2])
    except IndexError:
        print("Too little input commands!\n")
        sys.exit([2])
    except Exception:
        print("""Encountered an error while processing input\n
        Correct input should look like this: join file_path file_path column_name join_type\n
        If the column type has white space in its name, write it in quotation.\n
        """)
        sys.exit([2])
    return input_command[0], input_command[1], input_command[2], input_command[3]


filename1, filename2, column_name, used_type = user_input()

duplicates = look_for_duplicates(file_name1=filename1, file_name2=filename2, merge_column=column_name)

for line in merge_files(file_name1=filename1, file_name2=filename2, duplicate_list=duplicates, merge_type=used_type):
    print(line)
