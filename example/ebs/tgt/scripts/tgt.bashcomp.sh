# list available target names
_tgt_targets() {
    COMPREPLY=( $(compgen -W "$(tgt-admin --show|\
        grep Target | cut -d' ' -f3\
    ) ALL help" -- "${cur}") )
}


have tgtd &&
_tgtd() {
    local optslist split=false
    local cur prev

    COMPREPLY=()
    cur=$(_get_cword "=")
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    _expand || return 0

    optslist='
        --foreground -f
        --control-port -C
        --nr_iothreads -t
        --debug -d
        --version -V
        --help -h
    '

    _split_longopt && split=true

    case "${prev}" in
        --nr_iothreads|-t|\
        --control-port|-C|\
        --debug|-d)
            return 0;;
    esac

    $split && return 0

    case "${cur}" in
        -*)
            COMPREPLY=( $(compgen -W "${optslist}" -- "${cur}") )
            return 0;;
        *)
            _filedir
            return 0;;
    esac
} &&
complete -F _tgtd ${nospace} tgtd


have tgtimg &&
_tgtimg() {
    local optslist split=false
    local cur prev

    COMPREPLY=()
    cur=$(_get_cword "=")
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    _expand || return 0

    optslist='
        --help -h
        --op -o
        --device-type -Y
        --barcode -b
        --size -s
        --type -t
        --file -f
        --thin-provisioning -T
    '

    _split_longopt && split=true

    case "${prev}" in
        --op|-o)
            COMPREPLY=( $(compgen -W "new show" -- "${cur}") )
            return 0;;
        --device-type|-Y)
            COMPREPLY=( $(compgen -W "cd disk tape" -- "${cur}") )
            return 0;;
        --type|-t)
            COMPREPLY=( $(compgen -W "dvd+r disk data clean worm" -- "${cur}") )
            return 0;;
        --file|-f)
            _filedir
            return 0;;
        --barcode|-b|\
        --size|-s)
            return 0;;
    esac

    $split && return 0

    case "${cur}" in
        -*)
            COMPREPLY=( $(compgen -W "${optslist}" -- "${cur}") )
            return 0;;
        *)
            _filedir
            return 0;;
    esac
} &&
complete -F _tgtimg ${nospace} tgtimg


have tgtadm &&
_tgtadm() {
    local optslist split=false
    local cur prev

    COMPREPLY=()
    cur=$(_get_cword "=")
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    _expand || return 0

    optslist='
        --debug -d
        --help -h
        --version -V
        --lld -L
        --op -o
        --mode -m
        --tid -t
        --sid -s
        --cid -c
        --lun -l
        --name -n
        --value -v
        --backing-store -b
        --bstype -E
        --bsoflags -f
        --blocksize -y
        --targetname -T
        --initiator-address -I
        --initiator-name -Q
        --user -u
        --password -p
        --host -H
        --force -F
        --params -P
        --bus -B
        --device-type -Y
        --outgoing -O
        --control-port -C
    '

    _split_longopt && split=true

    case "${prev}" in
        --lld|-L)
            COMPREPLY=( $(compgen -W "iscsi iser" -- "${cur}") )
            return 0;;
        --op|-o)
            COMPREPLY=( $(compgen -W "new delete bind unbind show
                update stat start stop" -- "${cur}") )
            return 0;;
        --mode|-m)
            COMPREPLY=( $(compgen -W "system sys target tgt logicalunit lu
                portal pt session sess connection conn account lld" -- "${cur}") )
            return 0;;
        --bstype|-E)
            COMPREPLY=( $(compgen -W "rdwr aio rbd sg ssc" -- "${cur}") )
            return 0;;
        --bsoflags|-f)
            COMPREPLY=( $(compgen -W "direct sync" -- "${cur}") )
            return 0;;
        --device-type|-Y)
            COMPREPLY=( $(compgen -W "disk tape cd changer osd ssc pt" -- "${cur}") )
            return 0;;
        --targetname|-T)
            _tgt_targets
            return 0;;
        --backing-store|-b)
            _filedir
            return 0;;
        --blocksize|-y|\
        --bus|-B|\
        --cid|-c|\
        --control-port|-C|\
        --host|-H|\
        --initiator-address|-I|\
        --initiator-name|-Q|\
        --lun|-l|\
        --name|-n|\
        --params|-P|\
        --password|-p|\
        --sid|-s|\
        --tid|-t|\
        --user|-u|\
        --value|-v)
            return 0;;
    esac

    $split && return 0

    case "${cur}" in
        -*)
            COMPREPLY=( $(compgen -W "${optslist}" -- "${cur}") )
            return 0;;
        *)
            _filedir
            return 0;;
    esac
} &&
complete -F _tgtadm ${nospace} tgtadm


have tgt-admin &&
_tgt_admin() {
    local optslist split=false
    local cur prev

    COMPREPLY=()
    cur=$(_get_cword "=")
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    _expand || return 0

    optslist='
        --execute -e
        --delete
        --offline
        --ready
        --update
        --show -s
        --conf -c
        --ignore-errors
        --force -f
        --pretend -p
        --dump
        --verbose -v
        --help -h
        --control-port -C
    '

    _split_longopt && split=true

    case "${prev}" in
        --delete|-d|\
        --offline|\
        --ready|\
        --update)
            _tgt_targets
            return 0;;
        --conf|-c)
            _filedir
            return 0;;
        --control-port|-C)
            return 0;;
    esac

    $split && return 0

    case "${cur}" in
        -*)
            COMPREPLY=( $(compgen -W "${optslist}" -- "${cur}") )
            return 0;;
        *)
            _filedir
            return 0;;
    esac
} &&
complete -F _tgt_admin ${nospace} tgt-admin


have tgt-setup-lun &&
_tgt_setup_lun() {
    local optslist split=false
    local cur prev

    COMPREPLY=()
    cur=$(_get_cword "=")
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    _expand || return 0

    optslist='
        -d
        -n
        -b
        -t
        -C
        -h
    '

    _split_longopt && split=true

    case "${prev}" in
        -n)
            _tgt_targets
            return 0;;
        -d)
            _filedir
            return 0;;
    esac

    $split && return 0

    case "${cur}" in
        -*)
            COMPREPLY=( $(compgen -W "${optslist}" -- "${cur}") )
            return 0;;
        *)
            _filedir
            return 0;;
    esac
} &&
complete -F _tgt_setup_lun ${nospace} tgt-setup-lun
