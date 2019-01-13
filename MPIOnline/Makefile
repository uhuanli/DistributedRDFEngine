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
INCS_DIRS  =-I${USR_DIR}/include -I${THRIFT_DIR}/include/thrift -I/usr/liyouhuan/cluster/MPICH/include
LIBS_DIRS  =-L${USR_DIR}/lib -L${THRIFT_VER}/lib -L/usr/liyouhuan/cluster/MPICH/lib
CPP_DEFS   =-D=HAVE_CONFIG_H
CPP_OPTS   =-Wall -O2
LIBS       =-lthrift

GEN_SRC    = ./gen-cpp/Hbase.cpp  \
             ./gen-cpp/Hbase_types.cpp   \
             ./gen-cpp/Hbase_constants.cpp	\
	     ./HBASE.cpp \
	     ./Global.cpp \
	     ./DatabaseHBase.cpp \
	     ./Database.cpp \
	     ./STREE.cpp \
	     ./Query.cpp \
	     ./Type.cpp

GEN_INC    = -I./gen-cpp -I./

default: main

main: 
	mpic++ ${CPP_OPTS} ${CPP_DEFS} -o main ${GEN_INC} ${INCS_DIRS} main.cpp ${GEN_SRC} ${LIBS_DIRS} ${LIBS}

clean:
	rm -rf main
