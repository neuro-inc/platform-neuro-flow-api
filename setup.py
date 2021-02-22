from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.7.3",
    "neuro_auth_client==21.1.6",
    "trafaret==2.0.2",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "sentry-sdk==0.20.2",
)

setup(
    name="platform-neuro-flow-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-neuro-flow-api",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": ["platform-neuro-flow-api=platform_neuro_flow_api.api:main"]
    },
    zip_safe=False,
)
