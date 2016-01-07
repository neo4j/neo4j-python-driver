import os
import tarfile

def clean_up():
    dir_path = (os.path.dirname(os.path.realpath(__file__)))
    files = os.listdir(dir_path)
    for f in files:
        if not os.path.isdir(f) and ".feature" in f:
            os.remove(os.path.join(dir_path, f))


def set_up():
    dir_path = (os.path.dirname(os.path.realpath(__file__)))
    url = "https://s3-eu-west-1.amazonaws.com/remoting.neotechnology.com/driver-compliance/tck.tar.gz"
    file_name = url.split('/')[-1]
    _download_tar(url,file_name)

    tar = tarfile.open(file_name)
    tar.extractall(dir_path)
    tar.close()
    os.remove(file_name)


def _download_tar(url, file_name):
    try:
        import urllib2
        tar = open(file_name, 'w')
        response = urllib2.urlopen(url)
        block_sz = 1024
        while True:
            buffer = response.read(block_sz)
            if not buffer:
                break
            tar.write(buffer)
        tar.close()
    except ImportError:
        from urllib import request
        request.urlretrieve(url, file_name)