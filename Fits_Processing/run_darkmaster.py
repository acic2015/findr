__author__ = 'asherkhb'
from os import system
import config_writers

def runDarkmaster(image_dict, darklist_filename, masterdark_filename, norm_filename,
                  bot_xo=None, bot_xf=None, bot_yo=None, bot_yf=None,
                  top_xo=None, top_xf=None, top_yo=None, top_yf=None,
                  width=None, height=None,
                  config=None, medianNorm=False, medianDark=False):

    # Write dark images to config file.
    darks = image_dict['DARK']
    config_writers.writeListCfg(darks, darklist_filename)
    # Fill out required parameters
    options = '--fileListFile=%s --darkFileName=%s --normFileName=%s' % (darklist_filename,
                                                                         masterdark_filename,
                                                                         norm_filename)
    # Fill out bottom/top normalization coordinates, if present.
    if bot_xo and bot_xf and bot_yo and bot_yf and top_xo and top_xf and top_yo and top_yf:
        options += ' --bot_xo=%s --bot_xf=%s --bot_yo=%s --bot_yf=%s' % (str(bot_xo), str(bot_xf),
                                                                         str(bot_yo), str(bot_yf))
        options += ' --top_xo=%s --top_xf=%s --top_yo=%s --top_yf=%s' % (str(top_xo), str(top_xf),
                                                                         str(top_yo), str(top_yf))
    # Fill out height/width of centered normalization region (overrides normalization coordinates), if present.
    if width and height:
        options += ' --width=%s --height=%s' % (str(width), str(height))
    # Add median options, if present
    if medianNorm:
        options += ' --medianNorm'
    if medianDark:
        options += ' --medianDark'
    # Build & call darkmaster command.
    cmd = 'darkmaster ' + options
    system(cmd)


#images = {"DARK": [], "SCIENCE": []}
#runDarkmaster(images, 'darks.list', 'masterdark.fits', 'norms.dat')