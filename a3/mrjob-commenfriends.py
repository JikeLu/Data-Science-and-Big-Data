from mrjob.job import MRJob
from mrjob.step import MRStep

class CommonFriends(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_pairs,
                   reducer=self.reducer_common_friends)
        ]

    def mapper_pairs(self, _, line):
        person, friends_list = line.split(' : ')
        friends = friends_list.split()
        
        for friend in friends:
            yield (tuple(sorted((person, friend))), list(friends))  

    def reducer_common_friends(self, key, values):
        common_friends = list(values)
        if len(common_friends) > 1:
            shared_friends = set.intersection(*[set(friends) for friends in common_friends])
            final_friends = shared_friends - set(key)
            if final_friends:
                yield key, list(final_friends)

if __name__ == '__main__':
    CommonFriends.run()
