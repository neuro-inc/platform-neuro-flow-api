from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)


install_requires = (
    "aiohttp==3.7.3",
    "neuro_auth_client==21.4.19",
    "platform-logging==21.5.13",
    "aiohttp-cors==0.7.0",
    "aiozipkin==1.1.0",
    "sentry-sdk==1.0.0",
    "marshmallow==3.10.0",
    "aiohttp-apispec==2.2.1",
    "alembic==1.5.4",
    "psycopg2-binary==2.8.6",
    "asyncpgsa==0.27.1",
    "sqlalchemy~=1.3.0",
)

setup(
    name="platform-neuro-flow-api",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    url="https://github.com/neuromation/platform-neuro-flow-api",
    packages=find_packages(),
    install_requires=install_requires,
    setup_requires=setup_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": ["platform-neuro-flow-api=platform_neuro_flow_api.api:main"]
    },
    zip_safe=False,
)
