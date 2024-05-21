from setuptools import setup, find_packages

setup(
    name='cloudify',  # The name of your package
    version='0.1.0',  # The initial release version
    packages=find_packages(),  # Automatically find packages in your project
#    scripts=['myscript.py'],  # The standalone scripts to be installed
    install_requires=[
        # List your project's dependencies here.
        # Example: 'numpy', 'requests',
    ],
 #   entry_points={
 #       'console_scripts': [
 #           'myscript=myscript:main',  # Create a command line entry point
 #       ],
 #   },
    author='Fabian Wachsmann',
    description='Xpublish plugins and apps for hosting lustre data via Open stack cloud vm',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://gitlab.dkrz.de/data-infrastructure-services/xpublish',  # URL of your project's repository
    classifiers=[
        'Programming Language :: Python :: 3',
#        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',  # Specify the Python versions you support
)
