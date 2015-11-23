__author__ = 'asherkhb'
import os.path
import multiprocessing as mp
import pprint

# Location of darksub and fitscent. If in path can leave, otherwise give path here.
darksub = 'darksub'
fitscent = 'fitscent'

# File Shifts
file_shifts = 'file_shifts.txt'

# Maximum number of parallel processes.
max_processes = 2

# Just some sample testing data.
images = {"DARK": [], "SCIENCE": ['V47_20141104053022231159.fits',
                                  'V47_20141104053022514640.fits',
                                  'V47_20141104053022798116.fits',
                                  'V47_20141104053023932047.fits',
                                  'V47_20141104053024215509.fits',
                                  'V47_20141104053024782464.fits',
                                  'V47_20141104053025065946.fits',
                                  'V47_20141104053026766813.fits',
                                  'V47_20141104053027050292.fits']}


def prependToFilename(filename, prepending):
    """
    Prepend Text to Filename.
    :param filename: Filename or path/to/filename to be modified.
    :param prepending: String to prepend to filename.
    :return: Modified filename or path/to/filename.
    """
    b = os.path.basename(filename)
    n = prepending + b
    return filename.replace(b, n)


def spawnDsubCmd(science_img, dark_img, norm_bot=None, norm_top=None):
    """
    Spawn a darksub command.
    :param science_img: Science image filename or path/to/filename.
    :param dark_img: Master dark filename or path/to/filename.
    :param norm_bot: Multiplicative scaling to apply to the bottom amplifier (optional).
    :param norm_top: Multiplicative scaling to apply to the top amplifier (optional).
    :return: darksub_command, subtracted_fiilename
    """
    dsub_out = prependToFilename(science_img, 'dsub_')
    dsub_opts = '--inputFile=%s --darkFile=%s --outputFile=%s' % (science_img, dark_img, dsub_out)
    if norm_bot:
        dsub_opts += ' --norm_bot=%s' % str(norm_bot)
    if norm_top:
        dsub_opts += ' --norm_top=%s' % str(norm_top)
    dsub_cmd = darksub + ' ' + dsub_opts
    return dsub_cmd, dsub_out


def spawnCentCmd(subtracted_img, xshift, yshift):
    """
    Spawn a fitscent command.
    :param subtracted_img: Dark subtracted science image.
    :param xshift: X shift to apply to image.
    :param yshift: Y shift to apply to image.
    :return: fitscent_command, centered_filename
    """
    cent_out = prependToFilename(subtracted_img, 'cent_')
    cent_opts = '--input=%s --x=%s --y=%s --output=%s' % (subtracted_img, str(xshift), str(yshift), cent_out)
    cent_cmd = fitscent + ' ' + cent_opts
    return cent_cmd, cent_out


def loadShifts(shifts_file):
    shifts = {}
    with open(shifts_file, 'r') as s:
        for l in s:
            c = l.split()
            shifts[c[0]] = {'x': c[1], 'y': c[2]}
    return shifts


def getNorms(img):  # TODO
    """

    :param img: Image to obtain normalization s for.
    :return:
    """
    top = ''
    bot = ''
    return top, bot


def getShifts(img, fileshifts):  # TODO
    """

    :param img: image to get shift values
    :return: xshift, yshift
    """
    xs = fileshifts[img]['x']
    ys = fileshifts[img]['y']
    return xs, ys


def runProcess(call):
    print call
    os.system(call)
    return 1


def subtractAndCenter(image_dict, masterdark, shifts_file):
    # Build list of science images to process.
    sciences = image_dict['SCIENCE']
    # Load shift values from file to memory.
    fileshifts = loadShifts(shifts_file)
    # Define necessary variables.
    scmds = []
    souts = []
    ccmds = []
    couts = []

    # Build up commands for each science image.
    for img in sciences:
        # Get norm and shift values.
        tnorm, bnorm = getNorms(img)
        xshift, yshift = getShifts(img, fileshifts)

        # Build subtraction task.
        ds_cmd, ds_out = spawnDsubCmd(img, masterdark, norm_bot=bnorm, norm_top=tnorm)
        # subtractions[img] = {'cmd': ds_cmd, 'out': ds_out}
        scmds.append(ds_cmd)
        souts.append(ds_out)

        # Build centering task.
        cn_cmd, cn_out = spawnCentCmd(ds_out, xshift=xshift, yshift=yshift)
        # centerings[img] = {'cmd': cn_cmd, 'out': cn_out}
        ccmds.append(cn_cmd)
        couts.append(cn_out)

    # Execute subtraction tasks (parallel).
    sub_pool = mp.Pool(processes=max_processes)
    sub_pool.map(runProcess, scmds)

    # Execute centering tasks (parallel).
    cent_pool = mp.Pool(processes=max_processes)
    cent_pool.map(runProcess, ccmds)

    # Return list of final filenames.
    return couts


def main():
    cent_dsub_files = subtractAndCenter(images, 'masterdark.fits', file_shifts)
    print cent_dsub_files

if __name__ == '__main__':
    main()