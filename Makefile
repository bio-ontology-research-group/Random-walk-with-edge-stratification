all: deepwalk_stratified

deepwalk_stratified: deepwalk_stratified.cc
	g++ -Ofast -funroll-loops -I. -Wall -std=c++0x deepwalk_stratified.cc -o deepwalk_stratified -lboost_thread -lboost_system -lpthread
