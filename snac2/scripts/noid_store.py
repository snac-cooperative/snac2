import snac2.noid as noid

def backup_ark_ids(n, path, starting_at=0, is_fake=True, batch_size=1000):
    with open(path, "ab") as fh:
        while starting_at < n:
            ark_ids = noid.get_ark_ids(batch_size, is_fake)
            fh.writelines([id+"\n" for id in ark_ids])
            fh.flush()
            print "writing lines %d to %d" % (starting_at, starting_at + batch_size)
            starting_at += batch_size
    

if __name__=="__main__":
    import sys
    starting_at = 0
    batch_size = 1000
    if len(sys.argv) >= 5:
        batch_size = int(sys.argv[4])
    if len(sys.argv) >= 4:
        starting_at = int(sys.argv[3])
    backup_ark_ids(int(sys.argv[1]), sys.argv[2], is_fake=False, batch_size=batch_size, starting_at=starting_at)
            
        
