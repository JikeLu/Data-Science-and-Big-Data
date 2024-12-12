from mrjob.job import MRJob
from mrjob.step import MRStep

class MRPageRank(MRJob):

    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg('--nodes', type=int, help='Total number of nodes')
        self.add_passthru_arg('--beta', type=float, default=0.85, help='Damping factor')
        self.add_passthru_arg('--iter', type=int, default=10, help='Number of iterations')

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer
            ) for _ in range(self.options.iter)
        ]

    def mapper_init(self):
        # Initialize PageRank as 1/N for each node
        self.rank = 1.0 / self.options.nodes

    def mapper(self, _, line):
        # Ensure the line is a string, converting lists and handling unexpected data types
        if isinstance(line, list):
            line = ' '.join(map(str, line))  # Convert all items to strings before joining
        
        parts = line.split()
        node = parts[0]
        out_links = parts[1:]
        num_out_links = len(out_links)
        
        # Emit the structure of the graph
        yield node, ('NODES', out_links)
        
        # Emit PageRank contributions to each out-link
        if num_out_links > 0:
            rank_contribution = self.rank / num_out_links
            for out_link in out_links:
                yield out_link, ('RANK', rank_contribution)

    def reducer(self, key, values):
        total_rank = 0
        out_links = []
        for value_type, value in values:
            if value_type == 'NODES':
                out_links = value
            elif value_type == 'RANK':
                total_rank += value
        
        # Calculate new PageRank using the damping factor
        new_rank = (1 - self.options.beta) / self.options.nodes + self.options.beta * total_rank
        self.rank = new_rank  # Update rank for next iteration
        yield key, (new_rank, out_links)

if __name__ == '__main__':
    MRPageRank.run()
