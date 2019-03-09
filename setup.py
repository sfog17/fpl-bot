from setuptools import setup, find_packages

setup(
    name='fpl-bot',
    version='0.1',
    url='https://github.com/sfog17/fpl-bot.git',
    author='sfog17',
    author_email='',
    description='Optimise team selection for Fantasy Football using machine learning',
    install_requires=['requests', 'numpy', 'pandas'],
    packages=['fpl']
)
