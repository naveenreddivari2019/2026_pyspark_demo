
def args_exmpl(*args, **kwargs):
    print("args:", args)
    print("kwargs:", kwargs)

    print("args :", args[1])
    print("kwargs :", kwargs['a'])

args_exmpl(1, 2, 3, a=4, b=5)