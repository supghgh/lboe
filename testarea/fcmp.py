#!/usr/bin/env python
from ConfigParser import ConfigParser
from optparse import OptionParser
from os import path
from time import time

# Generator yielding each set
def get_set_data(configFile):
    cfg = ConfigParser()
    # Read file into object
    cfg.read(configFile)
    for eachSection in cfg.sections():
        try:
            yield (cfg.get(eachSection, "File1"),
            cfg.get(eachSection, "File2"),
            cfg.get(eachSection, "Delimiter"),
            cfg.get(eachSection, "KeyIndex"))
        except:
            print "Error reading data from section : ", eachSection
            yield None
    
# Method to convert data extract into dictionary
def csv_to_dict(fd, ret, delimiter, primColIndex):
    for i in fd:
        vals = i.split(delimiter)
        ret[vals[primColIndex]] = vals

# Each line of the second file is compared against
# the value stored for the primary column key 
def compare(fd, delimiter, primColIndex):
    global dataDict
    file1_orphans = []
    unmatched     = []
    
    for i in fd:
        rowitems = i.split(delimiter)
        key = rowitems[primColIndex]
        try:
            refvals = dataDict[key]
        except KeyError:
            file1_orphans.append(i)
        else:
            for idx in range(len(refvals)):
                if rowitems[idx] != refvals[idx]:
                    unmatched.append((i, delimiter.join(refvals)))
                    break
            del dataDict[key]
            
    return unmatched, file1_orphans

# Parse the options passed to the script
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="filename",
                      help="Configuration file location",
                      metavar="FILE")
    (options, args) = parser.parse_args()
    
    # Check if configuration file exists
    if options.filename and\
        path.exists(options.filename) and\
        path.isfile(options.filename) and\
        path.splitext(options.filename)[-1].lower() in [".ini", ".cfg"]:
        # Read each section of the configuration file and start
        # comparison based on the input parameters
        cfg_gen = get_set_data(options.filename)
        while(True):
            try:
                file1, file2, delim, colIndex = cfg_gen.next()
                # Considering 0 start index
                colIndex = int(colIndex) - 1
                dataDict = dict()
                st  = time()
                # Convert and update dataDict with table contents
                fd2 = open(file2)
                csv_to_dict(fd2, dataDict, delim, colIndex)
                fd2.close()
                print "\nComparing : %s and %s" %(file1, file2)
                # Send file description for file1 to compare
                fd1 = open(file2)
                unmatched, file1_orphans = compare(fd1, delim, colIndex)
                print "Completed! Time taken to compare : %.3f seconds"%(time()-st)
                fd1.close()
            except StopIteration:
                print            
                break
            except Exception as ex:
                print "Exception : ", str(ex)
                break 
            
    else:
        print """Error!! Could not proceed:\nReasons may include:\n-> \
Configuration file not provided with -i option.\n-> Configuration file path \
is invalid.\n-> Configuration file type is NOT ini or cfg."""
        print "Usage : python %s -i <ConfigFilePath>"%(__file__)
    exit(0)

