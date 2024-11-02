import sys
import os
import boto3
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws

# Adiciona o diretório src ao caminho de importação
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
# Adiciona o diretório mocks ao caminho de importação
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../mocks")))