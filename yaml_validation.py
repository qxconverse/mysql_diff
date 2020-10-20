from __future__ import print_function
import yaml
import sys


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    # 当时键值对时
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                if len(value) == 0:
                    yield pre + [key, '{}']
                else:
                    for d in dict_generator(value, pre + [key]):
                        yield d
            elif isinstance(value, list):
                if len(value) == 0:
                    yield pre + [key, '[]']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            elif isinstance(value, tuple):
                if len(value) == 0:
                    yield pre + [key, '()']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            else:
                yield pre + [key, value]
    # 当为值的时候
    else:
        yield pre + [indict]


if __name__ == "__main__":
    f1 = open(sys.argv[1])
    f2 = open(sys.argv[2])
    json1 = yaml.load(f1)
    json2 = yaml.load(f2)
    result_1 = []
    result_2 = []
    for i in dict_generator(json1):
        result_1.append('.'.join(i[0:-1]))
    for i in dict_generator(json2):
        # result_2.append('.'.join(i[0:-1]) + ' : ' + str(i[-1]))
        result_2.append('.'.join(i[0:-1]))
    print(result_1)
    print(result_2)

    # 求差集，在1中但不在2中
    ret1_2 = list(set(result_1).difference(set(result_2)))
    # 求差集，在2中但不在1中
    ret2_1 = list(set(result_2).difference(set(result_1)))
    print("ret1_2:", ret1_2)
    print("ret2_1:", ret2_1)
    if len(ret1_2) > 0 or len(ret2_1) > 0:
        raise AssertionError
    print("校验成功，配置文件结构一致")
