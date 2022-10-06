#define N 1000
#include <vector>
#include <tuple>
#include <iostream>
#include <unordered_set>
int main()
{
    // std::unordered_set<std::tuple<int, int>> db;
    std::vector<std::tuple<int, int>> db;
    for (int i = 0; i < N; i++)
    {
        for (int j = i; j < N; j++)
        {
            db.push_back(std::make_tuple(i, j));
        }
    }
    std::cout << db.size();
}