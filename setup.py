from setuptools import setup, find_packages

setup(
    name="vsam-gen",
    version="1.0.0",
    description="AI-powered VSAM file generator from COBOL copybooks",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "faker>=18.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "mostlyai": ["mostlyai[local]"],
    },
    entry_points={
        "console_scripts": [
            "vsam-gen=vsam_gen.cli:main",
        ],
    },
)
