#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include <set>
#include <bitset>
#include <pthread.h>
#include <fstream>
#include <climits>
#include <random>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <boost/threadpool.hpp>
#include <cstring>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>

#define NUM_NODES 1000000
#define BUFFERSIZE 512
#define THREADS 32

#define NUMBER_WALKS 50
#define LENGTH_WALKS 20

using namespace std;
using namespace boost::threadpool;


struct Edge {
  unsigned int edge ;
  unsigned int node ;
} ;

unordered_map<unsigned int, vector<Edge>> graph ;

random_device rd;
mt19937 rng(rd());
uniform_int_distribution<int> uni(0,INT_MAX);


ofstream fout;
boost::mutex mtx;


void build_graph(string fname) {
  char buffer[BUFFERSIZE];
  graph.reserve(NUM_NODES) ;
  ifstream in(fname);
  while(in) {
    in.getline(buffer, BUFFERSIZE);
    if(in) {
      Edge e;
      unsigned int source = atoi(strtok(buffer, " "));
      e.node = atoi(strtok(NULL, " "));
      e.edge = atoi(strtok(NULL, " "));
      graph[source].push_back(e);
    }
  }
}

void walk(unsigned int source) {
  vector<vector<unsigned int>> walks(NUMBER_WALKS) ;
  if (graph[source].size()>0) { // if there are outgoing edges at all
    for (int i = 0 ; i < NUMBER_WALKS ; i++) {
      int count = LENGTH_WALKS ;
      int current = source ;
      walks[i].push_back(source) ;

      unordered_map<int, vector<int>> adj; // dict for edge startification, key:edge type, values:nodes of that edge type 	
      while (count > 0) {
	if (graph[current].size() > 0 ) { // if there are outgoing edges
          unsigned int temp_target, temp_edge;
          set<unsigned int> edges;  
	  for (unsigned int ii=0; ii < graph[current].size(); ii++){
	    Edge curr_edge = graph[current][ii];
	    temp_target = curr_edge.node;
	    temp_edge = curr_edge.edge;
	    adj[temp_edge].push_back(temp_target);
            edges.insert(temp_edge); //set for uniq edge types  
	  }

          vector<unsigned int> edges1;
          set<unsigned int>::iterator k;
          for (k = edges.begin(); k != edges.end(); ++k) //convert set to vector for easy indexing
              edges1.push_back(*k);

	  unsigned int rand_e = uni(rng) % edges1.size(); 
          unsigned int edge = edges1[rand_e]; 
          unsigned int rand_n = uni(rng) % adj[edge].size();
          int target = adj[edge][rand_n];  //stratify by edge
	  walks[i].push_back(edge);
	  walks[i].push_back(target);
	  current = target;
	} else {
	  int edge = INT_MAX ; // null edge
	  current = source;
	  walks[i].push_back(edge);
	  walks[i].push_back(current);
	}
	count--;
      }
    }
  }
  stringstream ss;
  for(vector<vector<unsigned int>>::iterator it = walks.begin(); it != walks.end(); ++it) {
    for(size_t i = 0; i < (*it).size(); ++i) {
      if(i != 0) {
	ss << " ";
      }
      ss << (*it)[i];
    }
    ss << "\n" ;
  }
  mtx.lock() ;
  fout << ss.str() ;
  fout.flush() ;
  mtx.unlock() ;
}

void generate_corpus() {
  pool tp(THREADS);
  for ( auto it = graph.begin(); it != graph.end(); ++it ) {
    unsigned int source = it -> first ;
    tp.schedule(boost::bind(&walk, source ) ) ;
  }
  cout << tp.pending() << " tasks pending." << "\n" ;
  tp.wait() ;
}

int main (int argc, char *argv[]) {
  cout << "Building graph from " << argv[1] << "\n" ;
  build_graph(argv[1]);
  cout << "Number of nodes in graph: " << graph.size() << "\n" ;
  cout << "Writing walks to " << argv[2] << "\n" ;
  fout.open(argv[2]) ;
  generate_corpus() ;
  fout.close() ;
}

