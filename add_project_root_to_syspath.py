import sys
import os

def main():

    project_supdir = os.path.abspath(os.path.join(os.path.dirname(__file__),
        os.pardir))
    if project_supdir not in sys.path:
        print('Project superdir not in sys.path')
        # Get site-packages dir from manually created file.
        try:
            with open('env_sitepackages_path') as f:
                env_sitepackages_path = f.readline().strip()
            pthfile_path = os.path.join(env_sitepackages_path, 'added.pth')
        except FileNotFoundError:
            raise Exception('env_sitepackages_path file needed.')
        with open(pthfile_path, mode='a') as f:
            f.write(project_supdir)
        print('Now it is!')
    else:
        print('Project superdir is in sys.path!')

if __name__ == '__main__':
    main()
