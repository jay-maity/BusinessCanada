import shutil, glob


def merge_files(inputpath, outputfile, filetype):
    """
    Merge files
    :param inputpath:
    :param outputfile:
    :param filetype:
    :return:
    """
    with open(outputfile, 'wb') as outfile:
        for filename in glob.glob(inputpath+'/*.'+filetype):
            if filename == outputfile:
                # don't want to copy the output into the output
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)


def delete_dir(dirname):
    """
    Delete entire diretory and its contents
    :param dirname:
    :return:
    """
    shutil.rmtree(dirname)