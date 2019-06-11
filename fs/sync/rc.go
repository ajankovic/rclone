package sync

import (
	"context"

	"github.com/ncw/rclone/fs/accounting"
	"github.com/ncw/rclone/fs/operations"
	"github.com/ncw/rclone/fs/rc"
)

func init() {
	for _, name := range []string{"sync", "copy", "move"} {
		name := name
		moveHelp := ""
		if name == "move" {
			moveHelp = "- deleteEmptySrcDirs - delete empty src directories if set\n"
		}
		rc.Add(rc.Call{
			Path:         "sync/" + name,
			AuthRequired: true,
			Fn: func(ctx context.Context, in rc.Params) (rc.Params, error) {
				return rcSyncCopyMove(ctx, in, name)
			},
			Title: name + " a directory from source remote to destination remote",
			Help: `This takes the following parameters

- srcFs - a remote name string eg "drive:src" for the source
- dstFs - a remote name string eg "drive:dst" for the destination
- enableTransferGroups - enables alternative accounting stats for tracking all file transfers
` + moveHelp + `

See the [` + name + ` command](/commands/rclone_` + name + `/) command for more information on the above.`,
		})
	}
}

// Sync/Copy/Move a file
func rcSyncCopyMove(ctx context.Context, in rc.Params, name string) (out rc.Params, err error) {
	srcFs, err := rc.GetFsNamed(in, "srcFs")
	if err != nil {
		return nil, err
	}
	dstFs, err := rc.GetFsNamed(in, "dstFs")
	if err != nil {
		return nil, err
	}
	createEmptySrcDirs, err := in.GetBool("createEmptySrcDirs")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	enableTransferGroups, err := in.GetBool("enableTransferGroups")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	if enableTransferGroups {
		var opt operations.ListJSONOpt
		opt.Recurse = true
		opt.FilesOnly = true
		err = in.GetStruct("opt", &opt)
		if rc.NotErrParamNotFound(err) {
			return nil, err
		}
		var fs []accounting.FileSize
		err = operations.ListJSON(ctx, srcFs, "", &opt, func(item *operations.ListJSONItem) error {
			fs = append(fs, accounting.FileSize{Name: item.Path, Size: item.Size})
			return nil
		})
		accounting.Stats.GroupFiles(ctx, fs)
	}
	switch name {
	case "sync":
		return nil, Sync(ctx, dstFs, srcFs, createEmptySrcDirs)
	case "copy":
		return nil, CopyDir(ctx, dstFs, srcFs, createEmptySrcDirs)
	case "move":
		deleteEmptySrcDirs, err := in.GetBool("deleteEmptySrcDirs")
		if rc.NotErrParamNotFound(err) {
			return nil, err
		}
		return nil, MoveDir(ctx, dstFs, srcFs, deleteEmptySrcDirs, createEmptySrcDirs)
	}
	panic("unknown rcSyncCopyMove type")
}
