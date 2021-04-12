CC=g++
CPPFLAGS=--std=c++17
LDFLAGS=

SRC=file.cpp main.cpp
OBJ=$(subst .cpp,.o,$(SRC))

all: $(OBJ) 
	$(CC) $(LDFLAGS) -o mdb $(OBJ)

%.o: %.cpp
	$(CC) $(CPPFLAGS) -c $<

clean:
	$(RM) $(OBJ) mdb

.PHONY: clean all
