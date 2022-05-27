package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	"github.com/ipfs/go-cid"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"

	"github.com/ipfs/go-unixfsnode"

	"github.com/ipld/go-ipld-prime"

	carv2bs "github.com/ipld/go-car/v2/blockstore"

	"github.com/ipld/go-car/v2"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	sb "github.com/ipld/go-ipld-prime/traversal/selector/builder"

	"github.com/ipfs/go-unixfsnode/data/builder"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func main() {
	// ----------------    create UnixFS DAG in-memory
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageWriteOpener = storage.OpenWrite
	ls.StorageReadOpener = func(c linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		return storage.OpenRead(c, lnk)
	}
	rootLnk := createUnixFSDAG(&ls, "./1MB_testfile.txt")
	rootCid := rootLnk.(cidlink.Link).Cid
	fmt.Println("\n Finished creating unixFS DAG, Root multi-hash of the unixFS dag is", rootCid.Hash())

	// -------------- Get a CAR file showing the traversal of and also containing the first 256KB in the file from the DAG.

	// Notes:
	// 1 This requires us to change the go-car/v2 traversal code to load the root as a dag PB node so it can be reified as a unixfs file node.
	//     Have made the above change at https://github.com/ipld/go-car/pull/304/commits
	// 2. Something is wrong here, the CAR file size is 8MB even though we are ONLY selecting a 256KB RANGE !!!
	carbuf := createCarv1(&ls, rootCid)
	fmt.Println("Finished creating CAR file for range traversal on Unixfs dag, car file size is:", carbuf.Len())
	if err := ioutil.WriteFile("./car.txt", carbuf.Bytes(), 0777); err != nil {
		panic(err)
	}

	// --- Now from the CAR file, reconstruct a normal file which has the 256KB range we selected. A Saturn client would do this operation.
	bs, err := carv2bs.OpenReadOnly("./car.txt")
	if err != nil {
		panic(err)
	}
	rts, err := bs.Roots()
	if err != nil {
		panic(err)
	}
	fmt.Println("\n root hash in read only car bs is", rts[0].Hash())
	defer bs.Close()
	readPartialUnixFSFromCAR(bs, rootLnk)
}

func readPartialUnixFSFromCAR(bs blockstore.Blockstore, rootLink datamodel.Link) {
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: bs}
	ls.SetReadStorage(&bsa)
	unixfsnode.AddUnixFSReificationToLinkSystem(&ls)

	rootNode, err := ls.Load(ipld.LinkContext{}, rootLink, dagpb.Type.PBNode)
	if err != nil {
		panic(err)
	}

	ufn, err := file.NewUnixFSFile(context.Background(), rootNode, &ls)
	if err != nil {
		panic(err)
	}
	bz, err := ufn.AsBytes()
	// THIS PANICS WITH A BLOCK NOT FOUND ERROR
	if err != nil {
		panic(err)
	}
	fmt.Println("\n file bytes are", bz)
}

func createCarv1(ls *linking.LinkSystem, rootCid cid.Cid) *bytes.Buffer {
	sb := sb.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := sb.ExploreInterpretAs("unixfs", sb.MatcherSubset(0, 256*1000))
	unixfsnode.AddUnixFSReificationToLinkSystem(ls)
	var carbz bytes.Buffer
	_, err := car.TraverseV1(context.Background(), ls, rootCid, sel.Node(), &carbz, car.StoreIdentityCIDs(false), car.WithoutIndex(),
		carv2bs.AllowDuplicatePuts(false), carv2bs.UseWholeCIDs(false))
	if err != nil {
		panic(err)
	}
	return &carbz
}

func createUnixFSDAG(ls *linking.LinkSystem, name string) ipld.Link {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	st, err := f.Stat()
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n Size of normal file is:%d, creating UnixFS DAG now", st.Size())

	// create a UnixFS DAG
	rootLink, _, err := builder.BuildUnixFSFile(f, "", ls)
	if err != nil {
		panic(err)
	}
	return rootLink
}

/*var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}*/
