__author__ = 'asherkhb'


def writeListCfg(lst, cfgname):
    """
    Write out a config file from a list.
    - Entries: 'listItem\n'
    :param lst: List to be written as a config file.
    :param cfgname: Filename or path/to/filename for config file.
    :return: Config filename or path/to/filename
    """
    cfg_out = open(cfgname, 'w')
    for e in lst:
        cfg_out.write(str(e) + '\n')
    cfg_out.close()
    return cfgname


def writeDictCfg(dct, cfgname):
    """
    Write out a config file from a dictionary.
    - Entries: 'key=value\n'
    :param dct: Dictionary to be written as a config file.
    :param cfgname: Filename or path/to/filename for config file.
    :return: Config filename or path/to/filename
    """
    cfg_out = open(cfgname, 'w')
    for k, v in dct.iteritems():
        cfg_out.write('%s=%s\n' % (str(k), str(v)))
    cfg_out.close()
    return cfgname

#list = ['dark1', 'dark2', 'dark3', 'dark4']
#dict = {'key1':'val1', 'key2':'val2', 'key3':'val3'}

#writeListCfg(list, 'samplelist.cfg')
#writeDictCfg(dict, 'sampledict.cfg')