from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
sys.stdout = open('konigsberg.txt', 'w')
orig_stdout = sys.stdout

class MREulerPath(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_degrees,
                   reducer=self.reducer_sum_degrees),
            MRStep(reducer=self.reducer_check_eulerian_path)
        ]

    def mapper_count_degrees(self, _, line):
        vertices = line.split()
        for vertex in vertices:
            yield vertex, 1

    def reducer_sum_degrees(self, vertex, counts):
        total_degree = sum(counts)
        print(f"{vertex} {total_degree}", file=orig_stdout)
        yield None, (vertex, total_degree % 2) 

    def reducer_check_eulerian_path(self, _, vertex_degrees):
        odd_count = sum(degree for vertex, degree in vertex_degrees)
        if odd_count == 0 or odd_count == 2:
            print("True", file=orig_stdout)
        else:
            print("False", file=orig_stdout)

if __name__ == '__main__':
    MREulerPath.run()