#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
THRIFT_VER = /usr/liyouhuan/cluster/lib/Thrift_install
USR_DIR    = /usr
THRIFT_DIR =${THRIFT_VER}
INCS_DIRS  =-I${USR_DIR}/include -I${THRIFT_DIR}/include/thrift
LIBS_DIRS  =-L${USR_DIR}/lib -L${THRIFT_VER}/lib
CPP_DEFS   =-D=HAVE_CONFIG_H
CPP_OPTS   =-Wall -O2
LIBS       =-lthrift

GEN_SRC    = ./gen-cpp/Hbase.cpp  \
             ./gen-cpp/Hbase_types.cpp   \
             ./gen-cpp/Hbase_constants.cpp

GEN_INC    = -I./gen-cpp

default: DemoClient

DemoClient: DemoClient.cpp
	g++ ${CPP_OPTS} ${CPP_DEFS} -o DemoClient ${GEN_INC} ${INCS_DIRS} DemoClient.cpp ${GEN_SRC} ${LIBS_DIRS} ${LIBS}

clean:
	rm -rf DemoClient
